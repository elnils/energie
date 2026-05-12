"""
ENTSOG Transparency Platform — physical gas flows (no auth).

Strategy: auto-discover active German border points via /operatorpointdirections,
fetch flows for each, cache the point list for 7 days to avoid hammering discovery.

Additionally fetches aggregated supply/demand balance via /aggregatedData.

Cache invalidation: if ALL point fetches return empty/404, the cache file is
deleted so the next run re-discovers fresh. Individual 404s are skipped silently
— the point no longer exists (e.g. NS-1, NS-2, discontinued routes).

Docs: https://transparency.entsog.eu/api/archiveDirectories/8/api-manual/
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from core import http, paths


CACHE_FILE = os.path.join(paths.DATA_DIR, '_entsog_points.json')
DISCOVERY_REFRESH_DAYS = 7

# Fallback list used ONLY when discovery itself fails (network error, 5xx).
# These are verified-active points as of May 2026.
# Note: do NOT put discontinued points here — they cause 404s every run.
FALLBACK_POINTS = [
    {'id': 'de-tso-0001itp-00096exit', 'label': 'Mallnow / DE→PL (GASCADE Yamal)'},
    {'id': 'cz-tso-0001itp-00010entry', 'label': 'Waidhaus / CZ→DE (NET4GAS)'},
]

AGGREGATE_INDICATORS = ['Physical Flow', 'Nomination', 'Firm Technical']
AGGREGATE_COUNTRIES = ['DE', 'EU']


def _iso_day(d: datetime) -> str:
    return d.strftime('%Y-%m-%d')


def _load_cached_points() -> Optional[dict]:
    if not os.path.exists(CACHE_FILE):
        return None
    try:
        with open(CACHE_FILE) as f:
            cache = json.load(f)
        discovered_at = datetime.fromisoformat(cache.get('discovered_at', '2000-01-01'))
        age_days = (datetime.now(timezone.utc) - discovered_at).days
        if age_days < DISCOVERY_REFRESH_DAYS:
            return cache
    except Exception:
        return None
    return None


def _save_cached_points(points: List[dict]) -> None:
    try:
        os.makedirs(paths.DATA_DIR, exist_ok=True)
        with open(CACHE_FILE, 'w') as f:
            json.dump({
                'discovered_at': datetime.now(timezone.utc).isoformat(),
                'points': points,
            }, f)
    except Exception as e:
        print(f'  ! entsog cache write: {e}')


def _invalidate_cache() -> None:
    try:
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
            print('    entsog: cache invalidated — will re-discover next run')
    except OSError:
        pass


def _discover_points() -> List[dict]:
    """
    Discover active German border points via /operatorpointdirections.
    Returns up to 15 points, imports (entry) first.

    Robust field-name handling: ENTSOG API has changed field names across versions.
    We try all known variants and match 'de' (case-insensitive) in any country field.
    hasData filter is intentionally relaxed — we check actual data in _fetch_point.
    """
    s = http.get_session()
    r = s.get(
        'https://transparency.entsog.eu/api/v1/operatorpointdirections.json',
        params={'limit': '-1'},
        timeout=60,
    )
    r.raise_for_status()
    payload = r.json()
    items = payload.get('operatorpointdirections') or payload.get('data') or []
    print(f'    entsog discovery: {len(items)} total operator-point-directions')

    # Debug: log field names from first item
    if items and isinstance(items[0], dict):
        sample_keys = list(items[0].keys())
        print(f'    entsog discovery sample fields: {sample_keys[:12]}')

    de_points = []
    seen = set()
    for it in items:
        if not isinstance(it, dict):
            continue

        # Collect ALL string values in this item and check for 'de' or 'DEU'
        # Country can be in many fields depending on API version
        country_fields = [
            it.get('operatorCountryKey', ''),
            it.get('tSOCountryISO2', ''),
            it.get('pointCountryKey', ''),
            it.get('adjacentCountryKey', ''),
            it.get('tsoCountryCode', ''),
            it.get('countryKey', ''),
        ]
        country_vals = [str(f).strip().lower() for f in country_fields if f]
        is_de = any(v in ('de', 'deu', 'germany') or v.startswith('de-') for v in country_vals)

        # Also check if operator key starts with 'de-'
        op_key_raw = str(it.get('operatorKey') or it.get('tsoKey') or '').lower()
        if not is_de and op_key_raw.startswith('de-'):
            is_de = True

        if not is_de:
            continue

        # hasData: accept True, 'true', 1, 'yes', or missing (let fetch decide)
        has_data = it.get('hasData')
        if has_data in (False, 'false', 0, 'no'):
            continue  # only skip if explicitly marked as no-data

        op_key    = op_key_raw or it.get('operatorKey', '').lower()
        pt_key    = str(it.get('pointKey') or it.get('tpPointKey') or '').lower()
        direction = str(it.get('directionKey') or it.get('flowDirection') or '').lower()

        if not (op_key and pt_key and direction):
            continue

        pd_id = f'{op_key}{pt_key}{direction}'
        if pd_id in seen:
            continue
        seen.add(pd_id)

        label = str(it.get('pointLabel') or it.get('pointName') or pt_key.upper())[:60]
        label = f'{label} ({"Import" if direction == "entry" else "Export"})'
        de_points.append({'id': pd_id, 'label': label, 'operator': op_key})

    print(f'    entsog discovery: {len(de_points)} DE points found')
    # Imports first (gas coming in = most relevant for crisis monitoring)
    de_points.sort(key=lambda x: (0 if 'Import' in x['label'] else 1, x['label']))
    return de_points[:15]


def _fetch_point(point_id: str, days: int = 30) -> Optional[List[dict]]:
    """
    Fetch Physical Flow for one point-direction.
    Returns None on 404 (point discontinued) so caller can skip cleanly.
    Raises on other errors so caller logs them.
    """
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    s = http.get_session()
    r = s.get(
        'https://transparency.entsog.eu/api/v1/operationalData.json',
        params={
            'pointDirection': point_id,
            'from': _iso_day(start),
            'to': _iso_day(end),
            'indicator': 'Physical Flow',
            'periodType': 'day',
            'timezone': 'CET',
            'limit': '-1',
        },
        timeout=70,
    )
    # 404 = point no longer published — skip without noise
    if r.status_code == 404:
        return None
    r.raise_for_status()
    rows = r.json().get('operationalData') or r.json().get('data') or []
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            value = float(row['value'])
        except (KeyError, TypeError, ValueError):
            continue
        period = (row.get('periodFrom') or row.get('periodTo') or '')[:10]
        if not period:
            continue
        out.append({'date': period, 'v': value, 'unit': row.get('unit', 'kWh/d')})
    out.sort(key=lambda x: x['date'])
    return out


def _fetch_aggregated_balance(country: str, days: int = 60) -> dict:
    """Aggregated supply/demand balance per country (Physical Flow, Nomination, Firm Technical)."""
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    s = http.get_session()
    result = {}
    for indicator in AGGREGATE_INDICATORS:
        try:
            r = s.get(
                'https://transparency.entsog.eu/api/v1/aggregatedData.json',
                params={
                    'country': country,
                    'indicator': indicator,
                    'from': _iso_day(start),
                    'to': _iso_day(end),
                    'periodType': 'day',
                    'timezone': 'CET',
                    'limit': '-1',
                },
                timeout=60,
            )
            if r.status_code == 404:
                continue
            if not r.ok:
                continue
            rows = r.json().get('aggregatedData') or r.json().get('data') or []
            series = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                try:
                    val = float(row.get('value') or 0)
                except (TypeError, ValueError):
                    continue
                period = (row.get('periodFrom') or row.get('periodTo') or '')[:10]
                if not period:
                    continue
                series.append({
                    'date': period,
                    'v': round(val, 0),
                    'direction': (row.get('directionKey') or '').lower(),
                    'unit': row.get('unit', 'kWh/d'),
                })
            series.sort(key=lambda x: x['date'])
            key = indicator.lower().replace(' ', '_')
            result[key] = series
            print(f'    entsog/balance {country}/{indicator}: {len(series)} pts')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! entsog/balance {country}/{indicator}: {e}')
    return result


def fetch() -> dict:
    # ── 1. Get point list (cache or discover) ──────────────────────────────
    used_fallback = False
    cached = _load_cached_points()
    if cached and cached.get('points'):
        points_to_fetch = cached['points']
        print(f'    entsog: using {len(points_to_fetch)} cached points '
              f'(age <{DISCOVERY_REFRESH_DAYS}d)')
    else:
        try:
            points_to_fetch = _discover_points()
            if points_to_fetch:
                _save_cached_points(points_to_fetch)
                print(f'    entsog: discovered {len(points_to_fetch)} active DE points')
            else:
                print('  ! entsog discovery returned empty — using fallback')
                points_to_fetch = FALLBACK_POINTS
                used_fallback = True
        except Exception as e:
            print(f'  ! entsog discovery failed: {e} — using fallback')
            points_to_fetch = FALLBACK_POINTS
            used_fallback = True

    # ── 2. Fetch flow data for each point ──────────────────────────────────
    points: Dict[str, dict] = {}
    errors: List[str] = []
    got_404_count = 0

    for p in points_to_fetch:
        try:
            series = _fetch_point(p['id'])
            if series is None:
                # 404 — point discontinued; skip cleanly, don't count as error
                got_404_count += 1
                print(f'    entsog/{p["id"][:35]}: 404 (discontinued, skipping)')
                continue
            points[p['id']] = {'label': p['label'], 'series': series}
            last = series[-1] if series else None
            print(f'    entsog/{p["id"][:35]}: {len(series)} pts'
                  + (f', last={last["date"]}={last["v"]:.0f}' if last else ' (empty)'))
            time.sleep(0.4)
        except Exception as e:
            short_err = str(e)[:80]
            print(f'  ! entsog/{p["id"][:35]}: {short_err}')
            errors.append(f'{p["id"]}: {short_err}')

    # If every cached point was 404 → stale cache; invalidate and fallback gracefully
    if got_404_count == len(points_to_fetch) and cached and not used_fallback:
        _invalidate_cache()
        print('  ! entsog: all cached points 404 — cache invalidated, re-discover next run')

    # If we have NO data at all (not even partial) and nothing in errors, raise
    # so the store preserves the previous good file.
    if not points and not errors:
        raise RuntimeError(
            f'ENTSOG: no data returned (all {len(points_to_fetch)} points skipped/404). '
            'Cache invalidated — discovery runs next cycle.'
        )

    # ── 3. Aggregated balance for DE + EU ──────────────────────────────────
    balance: Dict[str, dict] = {}
    for country in AGGREGATE_COUNTRIES:
        try:
            bal = _fetch_aggregated_balance(country)
            if bal:
                balance[country.lower()] = bal
        except Exception as e:
            print(f'  ! entsog/balance {country}: {e}')

    return {
        'data': {
            'points':  points,
            'balance': balance,
            'errors':  errors,
        },
        'meta': {
            'source':    'ENTSOG Transparency Platform',
            'license':   'free, attribution requested',
            'url':       'https://transparency.entsog.eu',
            'indicator': 'Physical Flow, daily',
            'units':     'kWh/d',
            'discovery': 'auto via /operatorpointdirections',
        },
    }
