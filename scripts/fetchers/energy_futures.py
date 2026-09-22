"""
energy_futures — EIA STEO + World Bank Pink Sheet + IMF WEO.

v5.4 fixes — the three sources that were dead in the data file:

  - EIA STEO eu_gas asked for series id NGEUIPRCNUS, which STEO does not
    publish ("EIA returned no rows: no rows (total=0)"). The main gas
    forecast chart, the TTF-Henry-Hub spread and the EU-gas row of the
    comparison table were therefore all empty. Series ids are now looked up
    in STEO's own seriesId facet by description, with the hard-coded ids
    kept as the first candidate.

  - World Bank returned 404: both fallback URLs point at superseded
    releases, and the release URL changes with every publication. Discovery
    now scans more landing pages with a wider pattern, and a failure is
    reported as an unavailable source with a reason instead of leaving the
    comparison table full of unexplained dashes.

  - IMF candidates POILBREN / POILAPSP / POILBRE all failed with an empty
    `values` object, i.e. none of those indicator ids exist any more. The
    indicator is now resolved against the Datamapper's published indicator
    list by label.

  All three now share one shape: try the known id, else ask the API what it
  actually offers and match on the human-readable description. What each one
  resolved to is written to meta.resolved.

v5.3 fixes:
  - EIA _fetch_eia_series: logs the EIA `warnings` field when 0 rows come
    back. EIA returns 200 OK with empty data for invalid filter combos and
    explains in warnings; without surfacing it the error message hid the
    real cause (wrong seriesId etc.).
  - WorldBank: discovery extended to two landing pages + two URL patterns.
    On 404 falls through cleanly to next candidate, doesn't abort. Discovery
    is logged so it's visible when the fallback list saved the run.
  - IMF: per output key now accepts a LIST of candidate indicator IDs, tries
    them in order, picks the first that returns non-empty data. Robust
    against IMF retiring/renaming indicators.
"""
from __future__ import annotations

import io
import json
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from core import http, paths


EIA_API_KEY = (os.environ.get('EIA_API_KEY') or '').strip()
EIA_STEO_URL = 'https://api.eia.gov/v2/steo/data/'

EIA_STEO_FACET_URL = 'https://api.eia.gov/v2/steo/facet/seriesId'

# Filled in during a run: which id each key actually resolved to.
EIA_RESOLVED: Dict[str, str] = {}
IMF_RESOLVED: Dict[str, str] = {}

# key -> (preferred series ids, regex patterns matched against STEO's own
# description of each series, exclusion patterns)
EIA_SERIES: Dict[str, tuple] = {
    'brent':  (('BREPUUS',),
               (r'brent.*spot', r'brent'),
               ()),
    'wti':    (('WTIPUUS',),
               (r'west texas intermediate.*spot', r'\bwti\b'),
               ()),
    'hh_gas': (('NGHHMCF', 'NGHHUUS'),
               (r'henry hub.*spot', r'henry hub'),
               ()),
    # NGEUIPRCNUS does not exist in STEO. The EU/TTF gas benchmark is
    # published under a different id that has changed across STEO releases,
    # so this one leans on the description match.
    'eu_gas': (('NGEUIPRCNUS',),
               (r'title transfer facility', r'\bttf\b',
                r'europe.*natural gas.*(price|spot)',
                r'natural gas.*europe.*(price|spot)',
                r'(europe|eu|dutch|netherlands).*gas.*price',
                r'gas price.*(europe|eu|dutch|netherlands)',
                r'natural gas.*price.*(europe|eu\b|dutch|netherlands)',
                r'import price.*natural gas'),
               (r'liquefied', r'lng')),
}

WB_LANDING_PAGES = [
    'https://www.worldbank.org/en/research/commodity-markets',
    'https://www.worldbank.org/en/research/commodity-markets/publication/commodity-markets-outlook',
    'https://thedocs.worldbank.org/en/doc/5d903e848db1d1b83e0ec8f744e55570-0350012021/related/CMO-Historical-Data-Monthly.xlsx',
    'https://openknowledge.worldbank.org/search?query=commodity%20markets%20outlook',
]
WB_FORECAST_FALLBACKS: List[str] = [
    'https://thedocs.worldbank.org/en/doc/24e8d315bdd05e6ba3c813bbd49b3358-0050012025/related/CMO-October-2025-Forecasts.xlsx',
    'https://thedocs.worldbank.org/en/doc/18675909112024025-0050022024/related/CMO-April-2025-Forecasts.xlsx',
]

IMF_BASE = 'https://www.imf.org/external/datamapper/api/v1'
# v5.3: per output key, list of candidate IMF indicator IDs in priority order.
# IMF has historically renamed/retired indicators between WEO releases.
IMF_INDICATOR_LIST_URL = f'{IMF_BASE}/indicators'
# key -> (preferred indicator ids, label patterns, label exclusions)
IMF_INDICATORS: Dict[str, tuple] = {
    'crude_oil': (('POILBREN', 'POILAPSP', 'POILBRE', 'POILAPSP_USD'),
                  (r'crude oil.*(brent|average)', r'\bcrude oil\b',
                   r'petroleum.*spot', r'oil price', r'\boil\b.*\bprice',
                   r'commodity.*oil'),
                  (r'\bgas\b',)),
    'eu_gas':    ((),
                  (r'natural gas.*(europe|eu)', r'\bnatural gas\b'),
                  (r'oil',)),
}

HISTORY_FILE = os.path.join(paths.DATA_DIR, 'history', 'energy_futures.jsonl')
COMMODITIES_FILE = os.path.join(paths.DATA_DIR, 'commodities.json')

PREV_SNAPSHOT_MIN_DAYS = 5
PREV_SNAPSHOT_MAX_DAYS = 30


def _now_utc() -> datetime:
    return datetime.now(paths.UTC)


def _current_month_str() -> str:
    return _now_utc().strftime('%Y-%m')


def _today_str() -> str:
    return _now_utc().strftime('%Y-%m-%d')


# ──────────────────────────────────────────────────────────────────────
# EIA STEO
# ──────────────────────────────────────────────────────────────────────

def _fetch_eia_series(series_id: str) -> List[Dict[str, Any]]:
    if not EIA_API_KEY:
        raise RuntimeError('EIA_API_KEY missing in environment')

    s = http.get_session()
    params = {
        'api_key': EIA_API_KEY,
        'frequency': 'monthly',
        'data[0]': 'value',
        'facets[seriesId][]': series_id,
        'sort[0][column]': 'period',
        'sort[0][direction]': 'asc',
        'offset': '0',
        'length': '5000',
    }
    r = s.get(EIA_STEO_URL, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()

    response = payload.get('response') or {}
    rows = response.get('data') or []
    if not rows:
        # FIX v5.3: surface EIA's diagnostic info instead of opaque "no rows"
        warnings = response.get('warnings') or []
        total    = response.get('total')
        msg = (response.get('error')
               or payload.get('error')
               or f'no rows (total={total}, warnings={warnings})')
        raise RuntimeError(f'EIA returned no rows: {msg}')

    cutoff = _current_month_str()
    out: List[Dict[str, Any]] = []
    for row in rows:
        period = row.get('period')
        raw_value = row.get('value')
        if not period or raw_value in (None, ''):
            continue
        try:
            v = float(raw_value)
        except (TypeError, ValueError):
            continue
        out.append({
            'period': period,
            'v': round(v, 4),
            'type': 'forecast' if period >= cutoff else 'actual',
        })
    return out


def _log_near_misses(label: str, catalog: Dict[str, str], hints: tuple,
                     limit: int = 12) -> None:
    """
    Print catalog entries containing any of `hints`, so a failed match is
    diagnosable from the run log.

    The September 2026 run loaded 1469 STEO series and 132 IMF indicators and
    matched neither EU gas nor crude oil. That leaves two very different
    conclusions — our patterns are too narrow, or these APIs simply do not
    publish the series — and only the candidate list tells them apart.
    """
    if not catalog:
        return
    hits = [(cid, desc) for cid, desc in catalog.items()
            if any(h.lower() in (desc or '').lower() for h in hints)]
    if not hits:
        print(f'    {label}: no catalog entry mentions any of {hints}')
        return
    print(f'    {label}: {len(hits)} catalog entries mention {hints}, showing {min(limit, len(hits))}:')
    for cid, desc in hits[:limit]:
        print(f'        {cid} = {desc[:88]}')


def _match_catalog(catalog: Dict[str, str],
                   preferred: tuple,
                   patterns: tuple,
                   exclusions: tuple = ()) -> Optional[str]:
    """
    Pick an id out of {id: description}: an offered preferred id first, then
    the first id whose description matches a pattern and no exclusion.
    Patterns are tried in order, so the caller controls precedence.
    """
    for pid in preferred:
        if pid in catalog:
            return pid
    for pattern in patterns:
        rx = re.compile(pattern, re.IGNORECASE)
        for cid, desc in catalog.items():
            if not rx.search(desc or ''):
                continue
            if any(re.search(x, desc or '', re.IGNORECASE) for x in exclusions):
                continue
            return cid
    return None


_EIA_FACET_CACHE: Optional[Dict[str, str]] = None


def _eia_steo_catalog() -> Dict[str, str]:
    """
    {seriesId: description} for every series STEO publishes.

    EIA answers a request for a non-existent series id with 200 OK and an
    empty data array, so a wrong id is indistinguishable from an outage
    unless we can see the real list. One request, cached for the run.
    """
    global _EIA_FACET_CACHE
    if _EIA_FACET_CACHE is not None:
        return _EIA_FACET_CACHE
    catalog: Dict[str, str] = {}
    if EIA_API_KEY:
        try:
            s = http.get_session()
            r = s.get(EIA_STEO_FACET_URL, params={'api_key': EIA_API_KEY}, timeout=40)
            r.raise_for_status()
            for row in ((r.json().get('response') or {}).get('facets') or []):
                sid = row.get('id')
                if sid:
                    catalog[str(sid)] = str(row.get('name') or row.get('description') or '')
            print(f'    eia_steo: catalog has {len(catalog)} series')
        except Exception as e:
            print(f'  ! eia_steo/catalog: {str(e)[:140]}')
    _EIA_FACET_CACHE = catalog
    return catalog


def _fetch_all_eia(errors: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {key: [] for key in EIA_SERIES}
    catalog = _eia_steo_catalog()
    for key, (preferred, patterns, exclusions) in EIA_SERIES.items():
        series_id = _match_catalog(catalog, preferred, patterns, exclusions)
        if series_id is None and catalog:
            _log_near_misses(f'eia_steo/{key}', catalog,
                             ('natural gas', 'gas price') if 'gas' in key else ('brent', 'crude', 'wti'))
        if series_id is None:
            # No catalog (request failed) — fall back to the hard-coded id so
            # a transient facet-endpoint failure doesn't disable a series
            # that would otherwise work.
            series_id = preferred[0] if preferred else None
        if series_id is None:
            msg = f'eia_steo/{key}: no matching series in STEO catalog'
            print(f'  ! {msg}')
            errors.append(msg)
            continue
        if preferred and series_id != preferred[0]:
            print(f'    eia_steo/{key}: resolved to {series_id} '
                  f'("{catalog.get(series_id, "")[:70]}")')
        EIA_RESOLVED[key] = series_id
        try:
            data = _fetch_eia_series(series_id)
            result[key] = data
            last_actual = next((p for p in reversed(data) if p['type'] == 'actual'), None)
            last_fc = data[-1] if data else None
            print(
                f'    eia_steo/{key}: {len(data)} pts '
                f'(actual until {last_actual["period"] if last_actual else "—"}, '
                f'forecast to {last_fc["period"] if last_fc else "—"})'
            )
            time.sleep(0.3)
        except Exception as e:
            msg = f'eia_steo/{key} ({series_id}): {e}'
            print(f'  ! {msg}')
            errors.append(msg)
    return result


# ──────────────────────────────────────────────────────────────────────
# World Bank Pink Sheet
# ──────────────────────────────────────────────────────────────────────

def _discover_wb_forecast_url(session) -> Optional[str]:
    """Scrape WB pages for the newest CMO-*-Forecasts.xlsx link."""
    patterns = [
        re.compile(r'href="(https?://[^"]+CMO[^"]+Forecasts?\.xlsx)"', re.IGNORECASE),
        # Release URLs carry a per-publication hash, so only the tail is
        # predictable. Both hard-coded fallbacks are superseded releases that
        # now 404, which is why discovery has to carry this.
        re.compile(r'(https?://thedocs\.worldbank\.org/[^"\s)]+CMO[^"\s)]*[Ff]orecast[^"\s)]*\.xlsx)',
                   re.IGNORECASE),
    ]
    for landing in WB_LANDING_PAGES:
        try:
            r = session.get(landing, timeout=20)
            r.raise_for_status()
        except Exception as e:
            print(f'  ! worldbank/discover {landing[:60]}: {e}')
            continue
        for pat in patterns:
            matches = pat.findall(r.text)
            if matches:
                # Pick lexically-latest URL (works because URLs contain year/month)
                clean = sorted(set(m.replace('&amp;', '&') for m in matches),
                               reverse=True)[0]
                print(f'    worldbank/discover: found {clean[:80]}...')
                return clean
    print('  ! worldbank/discover: no forecast URL on any landing page')
    return None


def _is_nan(x: Any) -> bool:
    try:
        return x != x
    except Exception:
        return False


def _parse_wb_forecast_xlsx(xlsx_bytes: bytes) -> Dict[str, List[Dict[str, Any]]]:
    try:
        import pandas as pd
    except ImportError as e:
        raise RuntimeError(f'pandas required to parse Pink Sheet XLSX: {e}')

    sheets: Dict[str, Any] = pd.read_excel(
        io.BytesIO(xlsx_bytes),
        sheet_name=None,
        header=None,
        engine='openpyxl',
    )

    targets = {
        'crude_oil': [
            'crude oil, brent', 'oil, brent', 'crude oil ($/bbl, brent',
            'crude oil avg', 'brent crude',
        ],
        'eu_gas': [
            'natural gas, europe', 'european gas',
            'natural gas europe', 'gas, europe',
        ],
    }
    found: Dict[str, List[Dict[str, Any]]] = {k: [] for k in targets}

    for sheet_name, df in sheets.items():
        if df is None or df.empty:
            continue

        year_row_idx: Optional[int] = None
        for idx in range(min(25, len(df))):
            row = df.iloc[idx].tolist()
            year_cells = [
                int(c) for c in row
                if isinstance(c, (int, float)) and not _is_nan(c)
                and 1980 <= int(c) <= 2050
            ]
            if len(year_cells) >= 5:
                year_row_idx = idx
                break
        if year_row_idx is None:
            continue
        year_row = df.iloc[year_row_idx].tolist()

        for r in range(year_row_idx + 1, len(df)):
            row = df.iloc[r].tolist()
            label_cell = next(
                (c for c in row if isinstance(c, str) and c.strip()),
                None,
            )
            if not label_cell:
                continue
            label_lc = label_cell.strip().lower()
            for out_key, substrs in targets.items():
                if found[out_key]:
                    continue
                if not any(sub in label_lc for sub in substrs):
                    continue
                for year_cell, value_cell in zip(year_row, row):
                    if not (isinstance(year_cell, (int, float)) and not _is_nan(year_cell)):
                        continue
                    year = int(year_cell)
                    if not (1980 <= year <= 2050):
                        continue
                    if not (isinstance(value_cell, (int, float)) and not _is_nan(value_cell)):
                        continue
                    found[out_key].append({
                        'year': year,
                        'v': round(float(value_cell), 4),
                    })
                break

    for key in found:
        seen: Dict[int, float] = {}
        for pt in found[key]:
            seen[pt['year']] = pt['v']
        found[key] = [{'year': y, 'v': v} for y, v in sorted(seen.items())]

    return found


def _fetch_worldbank(errors: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    s = http.get_session()
    empty: Dict[str, List[Dict[str, Any]]] = {'crude_oil': [], 'eu_gas': []}

    candidate_urls: List[str] = []
    discovered = _discover_wb_forecast_url(s)
    if discovered:
        candidate_urls.append(discovered)
    candidate_urls.extend(WB_FORECAST_FALLBACKS)
    seen = set()
    ordered_urls = [u for u in candidate_urls if not (u in seen or seen.add(u))]

    last_err: Optional[str] = None
    for url in ordered_urls:
        try:
            r = s.get(url, timeout=60)
            # FIX v5.3: handle 404 explicitly instead of letting raise_for_status
            # poison the error message with the URL bytes
            if r.status_code == 404:
                last_err = f'404 at {url[:80]}'
                continue
            r.raise_for_status()
            if not r.content or len(r.content) < 5000:
                last_err = f'XLSX too small ({len(r.content)} bytes) at {url[:60]}'
                continue
            parsed = _parse_wb_forecast_xlsx(r.content)
            for k in parsed:
                print(f'    worldbank/{k}: {len(parsed[k])} pts')
            if any(parsed.values()):
                return parsed
            last_err = f'XLSX parsed but no rows matched at {url[:60]}'
        except Exception as e:
            last_err = f'{url[:60]}: {e}'
            continue

    if last_err:
        errors.append(f'worldbank: {last_err}')
        print(f'  ! worldbank: {last_err} (tried {len(ordered_urls)} URLs)')
    return empty


# ──────────────────────────────────────────────────────────────────────
# IMF Datamapper
# ──────────────────────────────────────────────────────────────────────

def _fetch_imf_indicator(indicator: str) -> List[Dict[str, Any]]:
    s = http.get_session()
    url = f'{IMF_BASE}/{indicator}'
    r = s.get(url, timeout=30)
    r.raise_for_status()
    payload = r.json()
    values_root = payload.get('values', {}).get(indicator, {})
    if not values_root:
        # FIX v5.3: dump payload structure so we can diagnose API drift
        top_keys = list(payload.keys())[:10]
        values_keys = list((payload.get('values') or {}).keys())[:10]
        raise RuntimeError(
            f'IMF response missing values.{indicator} '
            f'(top_keys={top_keys}, values_keys={values_keys})'
        )

    year_dict = next(iter(values_root.values())) if values_root else {}
    if not isinstance(year_dict, dict):
        raise RuntimeError(f'IMF response shape unexpected: {type(year_dict).__name__}')

    out: List[Dict[str, Any]] = []
    for year_str, raw in year_dict.items():
        try:
            year = int(year_str)
            v = float(raw)
        except (TypeError, ValueError):
            continue
        out.append({'year': year, 'v': round(v, 4)})
    out.sort(key=lambda x: x['year'])
    return out


_IMF_CATALOG_CACHE: Optional[Dict[str, str]] = None


def _imf_catalog() -> Dict[str, str]:
    """
    {indicator_id: label} from the Datamapper's own indicator list.

    Needed because the three ids we had been trying (POILBREN, POILAPSP,
    POILBRE) all came back with an empty `values` object — they no longer
    exist, and there is no way to guess the replacement without the list.
    """
    global _IMF_CATALOG_CACHE
    if _IMF_CATALOG_CACHE is not None:
        return _IMF_CATALOG_CACHE
    catalog: Dict[str, str] = {}
    try:
        s = http.get_session()
        r = s.get(IMF_INDICATOR_LIST_URL, timeout=40)
        r.raise_for_status()
        for iid, meta in (r.json().get('indicators') or {}).items():
            if isinstance(meta, dict):
                catalog[str(iid)] = str(meta.get('label') or meta.get('description') or '')
            else:
                catalog[str(iid)] = str(meta)
        print(f'    imf: catalog has {len(catalog)} indicators')
    except Exception as e:
        print(f'  ! imf/catalog: {str(e)[:140]}')
    _IMF_CATALOG_CACHE = catalog
    return catalog


def _fetch_all_imf(errors: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Try each candidate indicator ID per output key; first non-empty wins.
    Robust against IMF retiring/renaming indicators.
    """
    result: Dict[str, List[Dict[str, Any]]] = {'crude_oil': [], 'eu_gas': []}
    catalog = _imf_catalog()
    for key, (preferred, patterns, exclusions) in IMF_INDICATORS.items():
        # Preferred ids first (cheap when they still exist), then whatever
        # the catalog offers that matches the description.
        candidates: List[str] = [c for c in preferred if not catalog or c in catalog]
        matched = _match_catalog(catalog, preferred, patterns, exclusions)
        if matched and matched not in candidates:
            candidates.append(matched)
        if not candidates:
            _log_near_misses(f'imf/{key}', catalog,
                             ('oil', 'gas', 'commodity', 'price'))
            msg = (f'imf/{key}: no matching indicator among '
                   f'{len(catalog)} Datamapper indicators')
            print(f'  ! {msg}')
            errors.append(msg)
            continue
        for indicator in candidates:
            try:
                data = _fetch_imf_indicator(indicator)
                if data:
                    result[key] = data
                    IMF_RESOLVED[key] = indicator
                    print(f'    imf/{key} ({indicator}): {len(data)} pts, '
                          f'last={data[-1]["year"]} "{catalog.get(indicator, "")[:60]}"')
                    break  # first success wins
                print(f'    imf/{key} ({indicator}): empty, trying next candidate')
            except Exception as e:
                # Only record final failure
                if indicator == candidates[-1]:
                    msg = f'imf/{key}: all candidates failed ({len(candidates)} tried), last={indicator}: {e}'
                    print(f'  ! {msg}')
                    errors.append(msg)
                else:
                    print(f'    imf/{key} ({indicator}): {e}, trying next')
            time.sleep(0.3)
    return result


# ──────────────────────────────────────────────────────────────────────
# Spot prices from commodities.json
# ──────────────────────────────────────────────────────────────────────

def _read_commodities_spot() -> Dict[str, Any]:
    if not os.path.exists(COMMODITIES_FILE):
        print('  ! spot: commodities.json not yet present')
        return {}
    try:
        with open(COMMODITIES_FILE, 'r', encoding='utf-8') as f:
            blob = json.load(f)
    except Exception as e:
        print(f'  ! spot: cannot read commodities.json: {e}')
        return {}

    data = blob.get('data', blob) if isinstance(blob, dict) else {}

    def _quote(key: str) -> Optional[Dict[str, Any]]:
        item = data.get(key) if isinstance(data, dict) else None
        if not isinstance(item, dict):
            return None
        q = item.get('quote') or {}
        series = item.get('series') or []
        price = q.get('price')
        if price is None and series:
            price = series[-1].get('v')
        if price is None:
            return None
        return {
            'price': round(float(price), 4),
            'change_pct': q.get('change_pct'),
            'updated': q.get('updated') or (series[-1].get('t') if series else None),
        }

    out: Dict[str, Any] = {}
    mapping = {
        'brent_usd_bbl':    'brent_crude',
        'wti_usd_bbl':      'wti_crude',
        'natgas_usd_mmbtu': 'natural_gas',
        'ttf_eur_mwh':      'ttf_gas',
    }
    for out_key, src_key in mapping.items():
        q = _quote(src_key)
        if q:
            out[out_key] = q
    return out


# ──────────────────────────────────────────────────────────────────────
# Snapshot JSONL
# ──────────────────────────────────────────────────────────────────────

def _save_snapshot(record: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
    today = record['date']

    kept: List[str] = []
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if obj.get('date') != today:
                        kept.append(line)
        except Exception as e:
            print(f'  ! history read failed (will overwrite): {e}')

    kept.append(json.dumps(record, ensure_ascii=False))
    kept = _prune_snapshots(kept)
    tmp = HISTORY_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write('\n'.join(kept) + '\n')
    os.replace(tmp, HISTORY_FILE)


def _prune_snapshots(lines: List[str]) -> List[str]:
    """
    Thin out the snapshot archive: daily for the recent window, monthly
    before that.

    Each snapshot holds four complete STEO curves, roughly 66 KB, and one was
    appended every day with no pruning — the file had reached 8.1 MB and was
    rewritten in full on every commit, which in a repo that commits several
    times a day costs far more than its own size.

    What the archive is actually for decides what to keep:
      - _load_previous_snapshot() wants one 5-30 days old, so the recent
        window must stay daily;
      - the interesting long-run question is how a forecast was revised over
        months, for which one snapshot per month is plenty.

    Nothing is discarded inside the daily window, and no month ever loses its
    last snapshot.
    """
    daily_window_days = 45
    today = _now_utc().date()

    dated: List[tuple] = []
    for line in lines:
        try:
            obj = json.loads(line)
            d = datetime.strptime(obj.get('date', ''), '%Y-%m-%d').date()
        except (json.JSONDecodeError, ValueError):
            continue  # unparseable lines are dropped, they can't be read back
        dated.append((d, line))
    dated.sort(key=lambda x: x[0])

    keep_by_month: Dict[str, tuple] = {}
    recent: List[tuple] = []
    for d, line in dated:
        if (today - d).days <= daily_window_days:
            recent.append((d, line))
        else:
            keep_by_month[d.strftime('%Y-%m')] = (d, line)  # last of the month

    final = sorted(list(keep_by_month.values()) + recent, key=lambda x: x[0])
    dropped = len(dated) - len(final)
    if dropped:
        print(f'    energy_futures history: pruned {dropped} snapshots '
              f'({len(final)} kept: {len(recent)} daily + {len(keep_by_month)} monthly)')
    return [line for _d, line in final]


def _load_previous_snapshot() -> Dict[str, List[Dict[str, Any]]]:
    empty = {'eia_brent': [], 'eia_wti': [], 'eia_hh_gas': [], 'eia_eu_gas': []}
    if not os.path.exists(HISTORY_FILE):
        return empty

    now = _now_utc()
    candidates: List[Dict[str, Any]] = []
    try:
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                date_str = obj.get('date')
                if not date_str:
                    continue
                try:
                    dt = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=paths.UTC)
                except ValueError:
                    continue
                age_days = (now - dt).days
                if PREV_SNAPSHOT_MIN_DAYS <= age_days <= PREV_SNAPSHOT_MAX_DAYS:
                    candidates.append({'age': age_days, 'obj': obj})
    except Exception as e:
        print(f'  ! prev-week read failed: {e}')
        return empty

    if not candidates:
        return empty

    candidates.sort(key=lambda c: abs(c['age'] - 7))
    chosen = candidates[0]['obj']
    print(f'    previous_week: snapshot from {chosen.get("date")} (age {candidates[0]["age"]}d)')

    return {
        'eia_brent':  chosen.get('eia_brent', []),
        'eia_wti':    chosen.get('eia_wti', []),
        'eia_hh_gas': chosen.get('eia_hh_gas', []),
        'eia_eu_gas': chosen.get('eia_eu_gas', []),
    }


# ──────────────────────────────────────────────────────────────────────
# Main entrypoint
# ──────────────────────────────────────────────────────────────────────

def fetch() -> dict:
    errors: List[str] = []

    eia_data = _fetch_all_eia(errors)
    wb_data = _fetch_worldbank(errors)
    imf_data = _fetch_all_imf(errors)
    spot_data = _read_commodities_spot()
    prev_week = _load_previous_snapshot()

    eia_empty = all(not v for v in eia_data.values())
    wb_empty = all(not v for v in wb_data.values())
    imf_empty = all(not v for v in imf_data.values())
    if eia_empty and wb_empty and imf_empty:
        raise RuntimeError(
            f'energy_futures: ALL external APIs failed. Errors: '
            f'{"; ".join(errors) or "no detail"}'
        )

    if not eia_empty:
        try:
            _save_snapshot({
                'date': _today_str(),
                'eia_brent':  eia_data['brent'],
                'eia_wti':    eia_data['wti'],
                'eia_hh_gas': eia_data['hh_gas'],
                'eia_eu_gas': eia_data['eu_gas'],
            })
        except Exception as e:
            msg = f'snapshot write: {e}'
            print(f'  ! {msg}')
            errors.append(msg)

    # Per-source availability. Without this the comparison table had no way
    # to tell "this forecaster has not published that commodity" apart from
    # "we could not reach the forecaster", and rendered both as a dash.
    def _status(prefix: str, data: Dict[str, List[Dict[str, Any]]]) -> dict:
        reason = next((e for e in errors if e.startswith(prefix)), None)
        available = [k for k, v in data.items() if v]
        return {
            'available': bool(available),
            'series': available,
            'reason': reason,
        }

    sources = {
        'eia_steo':  _status('eia_steo', eia_data),
        'worldbank': _status('worldbank', wb_data),
        'imf':       _status('imf', imf_data),
    }

    return {
        'data': {
            'eia_steo':      eia_data,
            'worldbank':     wb_data,
            'imf':           imf_data,
            'spot':          spot_data,
            'previous_week': prev_week,
            'sources':       sources,
            'errors':        errors,
        },
        'meta': {
            'source': 'EIA STEO + World Bank Pink Sheet + IMF WEO',
            'eia_release_freq': 'monthly',
            'wb_release_freq': 'monthly bulletin, semi-annual full forecast (April/October)',
            'imf_release_freq': 'semi-annual (April/October WEO)',
            'license': 'EIA: public domain. WB Pink Sheet: CC BY-4.0. IMF: free with attribution.',
            'resolved': {'eia_steo': dict(EIA_RESOLVED), 'imf': dict(IMF_RESOLVED)},
            'note': ('Series ids are resolved against each API\'s own catalog '
                     'by description; meta.resolved records the winners so a '
                     'rename shows up here instead of as an empty chart.'),
        },
    }


if __name__ == '__main__':
    import pprint
    result = fetch()
    summary = {
        k: (len(v) if isinstance(v, (list, dict)) else type(v).__name__)
        for k, v in result['data'].items()
    }
    pprint.pp(summary)
