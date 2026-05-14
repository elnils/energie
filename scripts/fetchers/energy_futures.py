"""
energy_futures — EIA STEO + World Bank Pink Sheet + IMF WEO.

Combines three independent commodity-forecast sources into one file that the
frontend's Futures tab consumes:

  - EIA Short-Term Energy Outlook: monthly history + 18+ month forecast for
    Brent, WTI, Henry Hub, and EU imported natural gas. Released monthly.
  - World Bank Commodity Markets Outlook (Pink Sheet): annual forecasts for
    crude oil and EU natural gas, several years out. Released semi-annually
    (April/October), with monthly bulletin updates in between.
  - IMF Datamapper API: annual oil price (Brent) with WEO forecasts.
    Released semi-annually (April/October).

Spot prices are NOT re-fetched here — they are read from data/commodities.json
to avoid duplicate Yahoo Finance calls.

Previous-week snapshots live in data/history/energy_futures.jsonl. One row per
day, deduplicated by date. The frontend's grey-dashed "Prognose Vorwoche"
layer uses the entry closest to 7 days old in the [5, 30]-day window.

Failure model:
  - Each sub-source has its own try/except. Failures are non-fatal and recorded
    in data.errors[]. The frontend can show them.
  - If ALL THREE sources fail simultaneously, fetch() raises RuntimeError with
    the aggregated error message. store.py keeps the previous file (stale=true).

Auth:
  - EIA needs EIA_API_KEY from os.environ. Already a GitHub Secret.
  - World Bank and IMF are open / no key required.

References:
  - EIA API v2: https://api.eia.gov/v2/steo/data/
  - EIA STEO series catalog: https://www.eia.gov/opendata/browser/steo
  - WB Pink Sheet: https://www.worldbank.org/en/research/commodity-markets
  - IMF Datamapper API: https://www.imf.org/external/datamapper/api/v1
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


# ════════════════════════════════════════════════════════════
# Configuration
# ════════════════════════════════════════════════════════════

EIA_API_KEY = (os.environ.get('EIA_API_KEY') or '').strip()
EIA_STEO_URL = 'https://api.eia.gov/v2/steo/data/'

# Verified 2026-05-14 against https://www.eia.gov/opendata/browser/steo.
# `seriesId` facet values. Frequency: monthly.
EIA_SERIES: Dict[str, str] = {
    'brent':  'BREPUUS',       # Brent Spot Price, $/bbl
    'wti':    'WTIPUUS',       # West Texas Intermediate Spot, $/bbl
    'hh_gas': 'NGHHMCF',       # Henry Hub Natural Gas Spot, $/MMBtu
    'eu_gas': 'NGEUIPRCNUS',   # EU Imported Natural Gas, $/MMBtu
}

# World Bank Commodity Markets Outlook (Pink Sheet).
# The forecast XLSX has a document-ID in the URL that changes each release,
# so we scrape the landing page first for the latest link, then fall back to
# curated direct URLs. Most recent fallback first.
WB_LANDING = 'https://www.worldbank.org/en/research/commodity-markets'
WB_FORECAST_FALLBACKS: List[str] = [
    'https://thedocs.worldbank.org/en/doc/24e8d315bdd05e6ba3c813bbd49b3358-0050012025/related/CMO-October-2025-Forecasts.xlsx',
    'https://thedocs.worldbank.org/en/doc/18675909112024025-0050022024/related/CMO-April-2025-Forecasts.xlsx',
]

# IMF Datamapper REST. POILBREN = "Crude Oil (petroleum), Brent, U.K., $/bbl"
IMF_BASE = 'https://www.imf.org/external/datamapper/api/v1'
IMF_INDICATORS: Dict[str, str] = {
    'crude_oil': 'POILBREN',
    # IMF Datamapper has no dedicated EU natgas indicator with WEO-style
    # forecasts. We deliberately leave eu_gas empty for IMF.
}

# Local files. paths.DATA_DIR is a plain string in v5.
HISTORY_FILE = os.path.join(paths.DATA_DIR, 'history', 'energy_futures.jsonl')
COMMODITIES_FILE = os.path.join(paths.DATA_DIR, 'commodities.json')

# Previous-week window. ~7 days but tolerate up to 30 if cron skipped.
PREV_SNAPSHOT_MIN_DAYS = 5
PREV_SNAPSHOT_MAX_DAYS = 30


# ════════════════════════════════════════════════════════════
# Time helpers
# ════════════════════════════════════════════════════════════

def _now_utc() -> datetime:
    return datetime.now(paths.UTC)


def _current_month_str() -> str:
    return _now_utc().strftime('%Y-%m')


def _today_str() -> str:
    return _now_utc().strftime('%Y-%m-%d')


# ════════════════════════════════════════════════════════════
# Sub-source: EIA STEO
# ════════════════════════════════════════════════════════════

def _fetch_eia_series(series_id: str) -> List[Dict[str, Any]]:
    """
    Returns list of {period: 'YYYY-MM', v: float, type: 'actual'|'forecast'}.

    STEO publishes ~24 months of forecast in the same call as history. We
    classify by period vs current month because the response does not flag
    historical vs forecast explicitly.
    """
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
        msg = response.get('error') or payload.get('error') or 'no rows'
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


def _fetch_all_eia(errors: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """Run all configured STEO series fetches; collect errors non-fatally."""
    result: Dict[str, List[Dict[str, Any]]] = {key: [] for key in EIA_SERIES}
    for key, series_id in EIA_SERIES.items():
        try:
            data = _fetch_eia_series(series_id)
            result[key] = data
            last_actual = next(
                (p for p in reversed(data) if p['type'] == 'actual'),
                None,
            )
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


# ════════════════════════════════════════════════════════════
# Sub-source: World Bank Pink Sheet
# ════════════════════════════════════════════════════════════

def _discover_wb_forecast_url(session) -> Optional[str]:
    """Scrape landing page for newest CMO-*-Forecasts.xlsx link."""
    try:
        r = session.get(WB_LANDING, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print(f'  ! worldbank/discover: {e}')
        return None
    matches = re.findall(
        r'href="(https?://[^"]+CMO[^"]+Forecasts?\.xlsx)"',
        r.text,
        re.IGNORECASE,
    )
    if matches:
        clean = matches[0].replace('&amp;', '&')
        print(f'    worldbank/discover: found {clean[:80]}...')
        return clean
    return None


def _is_nan(x: Any) -> bool:
    """NaN-safe check that doesn't blow up on non-numeric."""
    try:
        return x != x  # NaN is the only value unequal to itself
    except Exception:
        return False


def _parse_wb_forecast_xlsx(xlsx_bytes: bytes) -> Dict[str, List[Dict[str, Any]]]:
    """
    Pink Sheet forecast XLSX has rows per commodity, columns per year.
    Scans all sheets without assuming header position: detects the year-header
    row by counting cells that look like 4-digit years, then matches subsequent
    rows by label substring against our targets.
    """
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

        # Detect year header row (within first 25 rows, has ≥5 year-like cells)
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

        # Match data rows below the year header
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
                    continue  # already populated
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
                break  # one target per row max

    # Sort + dedupe each output
    for key in found:
        seen: Dict[int, float] = {}
        for pt in found[key]:
            seen[pt['year']] = pt['v']
        found[key] = [{'year': y, 'v': v} for y, v in sorted(seen.items())]

    return found


def _fetch_worldbank(errors: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """Returns {'crude_oil': [{year, v}], 'eu_gas': [{year, v}]}."""
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
        print(f'  ! worldbank: {last_err}')
    return empty


# ════════════════════════════════════════════════════════════
# Sub-source: IMF Datamapper
# ════════════════════════════════════════════════════════════

def _fetch_imf_indicator(indicator: str) -> List[Dict[str, Any]]:
    """Returns list of {year, v} from IMF Datamapper API."""
    s = http.get_session()
    url = f'{IMF_BASE}/{indicator}'
    r = s.get(url, timeout=30)
    r.raise_for_status()
    payload = r.json()
    values_root = payload.get('values', {}).get(indicator, {})
    if not values_root:
        raise RuntimeError(f'IMF response missing values.{indicator}')

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


def _fetch_all_imf(errors: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {'crude_oil': [], 'eu_gas': []}
    for key, indicator in IMF_INDICATORS.items():
        try:
            data = _fetch_imf_indicator(indicator)
            result[key] = data
            last = data[-1] if data else None
            print(f'    imf/{key} ({indicator}): {len(data)} pts, last={last["year"] if last else "—"}')
            time.sleep(0.3)
        except Exception as e:
            msg = f'imf/{key} ({indicator}): {e}'
            print(f'  ! {msg}')
            errors.append(msg)
    return result


# ════════════════════════════════════════════════════════════
# Spot prices — recycled from data/commodities.json
# ════════════════════════════════════════════════════════════

def _read_commodities_spot() -> Dict[str, Any]:
    """Read spot prices from commodities.json. No Yahoo re-call."""
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


# ════════════════════════════════════════════════════════════
# Previous-week snapshot (JSONL append-only)
# ════════════════════════════════════════════════════════════

def _save_snapshot(record: Dict[str, Any]) -> None:
    """Append today's snapshot; replace if today already present (idempotent)."""
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
    tmp = HISTORY_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write('\n'.join(kept) + '\n')
    os.replace(tmp, HISTORY_FILE)


def _load_previous_snapshot() -> Dict[str, List[Dict[str, Any]]]:
    """Return snapshot closest to 7 days old in [5,30]-day window."""
    empty = {
        'eia_brent':  [],
        'eia_wti':    [],
        'eia_hh_gas': [],
        'eia_eu_gas': [],
    }
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


# ════════════════════════════════════════════════════════════
# Main entrypoint
# ════════════════════════════════════════════════════════════

def fetch() -> dict:
    """
    Orchestrates EIA + WB + IMF + spot + history snapshot.
    Conforms to the v5 fetcher contract: returns {'data': {...}, 'meta': {...}}.
    """
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

    return {
        'data': {
            'eia_steo':      eia_data,
            'worldbank':     wb_data,
            'imf':           imf_data,
            'spot':          spot_data,
            'previous_week': prev_week,
            'errors':        errors,
        },
        'meta': {
            'source': 'EIA STEO + World Bank Pink Sheet + IMF WEO',
            'eia_release_freq': 'monthly',
            'wb_release_freq': 'monthly bulletin, semi-annual full forecast (April/October)',
            'imf_release_freq': 'semi-annual (April/October WEO)',
            'license': 'EIA: public domain. WB Pink Sheet: CC BY-4.0. IMF: free with attribution.',
            'series_ids_eia': EIA_SERIES,
            'series_ids_imf': IMF_INDICATORS,
        },
    }


# ════════════════════════════════════════════════════════════
# Standalone debug entry
# ════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import pprint
    result = fetch()
    summary = {
        k: (len(v) if isinstance(v, (list, dict)) else type(v).__name__)
        for k, v in result['data'].items()
    }
    pprint.pp(summary)
