"""
ENTSOG Transparency Platform — physical gas flows (no auth).

Strategy: instead of hardcoding point-direction IDs (which 404 over time
as routes get discontinued, see Nord Stream 1+2), we auto-discover the
currently active German border points via the /operatorpointdirections
endpoint, then fetch flows for the top ones with the most data.

Cached point-list lives in data/_entsog_points.json so we don't hammer
the discovery endpoint every hour. We re-discover weekly or if all
fetches fail.

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

# Fallback list if discovery fails — known historical Mallnow point.
FALLBACK_POINTS = [
    {'id': 'de-tso-0001itp-00096exit', 'label': 'Mallnow / DE→PL (GASCADE)'},
]


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


def _discover_points() -> List[dict]:
    """
    Discover active German border points via /operatorpointdirections.
    Filter: country=DE, hasData=1, isPipelineInGroup=true, return the
    top ~10 by recent activity.
    """
    s = http.get_session()
    url = 'https://transparency.entsog.eu/api/v1/operatorpointdirections.json'
    params = {'limit': '-1'}
    r = s.get(url, params=params, timeout=60)
    r.raise_for_status()
    payload = r.json()
    items = payload.get('operatorpointdirections') or payload.get('data') or []
    print(f'    entsog discovery: {len(items)} total operator-point-directions')

    # Pick those whose pointKey or operator country is DE, and that have data
    de_points = []
    seen = set()
    for it in items:
        if not isinstance(it, dict):
            continue
        # Different field names exist across API versions. Be tolerant.
        op_country = (it.get('operatorCountryKey') or
                      it.get('tSOCountryISO2') or '').lower()
        pt_country = (it.get('pointCountryKey') or
                      it.get('adjacentCountryKey') or '').lower()
        # We want DE on at least one side of the connection
        if 'de' not in (op_country, pt_country):
            continue
        # Has data?
        has_data = it.get('hasData')
        if has_data not in (True, 'true', 1, '1'):
            continue
        # Build the point-direction key
        op_key = it.get('operatorKey', '').lower()
        pt_key = it.get('pointKey', '').lower()
        direction = (it.get('directionKey') or '').lower()
        if not (op_key and pt_key and direction):
            continue
        # Format the ID — same format ENTSOG uses for operationalData filter
        pd_id = f'{op_key}{pt_key}{direction}'
        if pd_id in seen:
            continue
        seen.add(pd_id)
        label = (it.get('pointLabel') or it.get('pointName')
                 or pt_key.upper())[:60]
        # Add direction hint to the label
        if direction == 'exit':
            label = f'{label} (Export)'
        elif direction == 'entry':
            label = f'{label} (Import)'
        de_points.append({
            'id': pd_id,
            'label': label,
            'operator': op_key,
        })

    # Sort: prefer entries (gas coming in is what matters for crisis monitoring)
    de_points.sort(key=lambda x: (0 if 'Import' in x['label'] else 1, x['label']))
    return de_points[:15]  # cap at 15 so daily fetch isn't insane


def _fetch_point(point_id: str, days: int = 30) -> List[dict]:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    s = http.get_session()
    params = {
        'pointDirection': point_id,
        'from': _iso_day(start),
        'to': _iso_day(end),
        'indicator': 'Physical Flow',
        'periodType': 'day',
        'timezone': 'CET',
        'limit': '-1',
    }
    r = s.get('https://transparency.entsog.eu/api/v1/operationalData.json',
              params=params, timeout=70)
    r.raise_for_status()
    payload = r.json()
    rows = payload.get('operationalData') or payload.get('data') or []
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            value = row.get('value')
            if value is None:
                continue
            value = float(value)
        except (TypeError, ValueError):
            continue
        period = (row.get('periodFrom') or row.get('periodTo') or '')[:10]
        if not period:
            continue
        unit = row.get('unit', 'kWh/d')
        out.append({'date': period, 'v': value, 'unit': unit})
    out.sort(key=lambda x: x['date'])
    return out


def fetch() -> dict:
    # 1) Get the point list (cached or discover fresh)
    cached = _load_cached_points()
    if cached and cached.get('points'):
        points_to_fetch = cached['points']
        print(f'    entsog: using {len(points_to_fetch)} cached points '
              f'(re-discover in <={DISCOVERY_REFRESH_DAYS}d)')
    else:
        try:
            points_to_fetch = _discover_points()
            if points_to_fetch:
                _save_cached_points(points_to_fetch)
                print(f'    entsog: discovered {len(points_to_fetch)} active DE points')
            else:
                print('  ! entsog discovery returned empty, falling back')
                points_to_fetch = FALLBACK_POINTS
        except Exception as e:
            print(f'  ! entsog discovery failed: {e}, using fallback')
            points_to_fetch = FALLBACK_POINTS

    # 2) Fetch flow data for each
    points: Dict[str, dict] = {}
    errors: List[str] = []
    for p in points_to_fetch:
        try:
            series = _fetch_point(p['id'])
            points[p['id']] = {'label': p['label'], 'series': series}
            last = series[-1] if series else None
            print(f'    entsog/{p["id"][:30]}: {len(series)} pts'
                  + (f', last={last["date"]}={last["v"]:.0f}' if last else ''))
            time.sleep(0.4)
        except Exception as e:
            short_err = str(e)[:80]
            print(f'  ! entsog/{p["id"][:30]}: {short_err}')
            errors.append(f'{p["id"]}: {short_err}')
            points[p['id']] = {'label': p['label'], 'series': []}

    # If all empty AND we used cached, invalidate cache so next run re-discovers
    if all(not pt['series'] for pt in points.values()) and cached:
        try:
            os.remove(CACHE_FILE)
            print('    entsog: cache invalidated (all fetches empty)')
        except OSError:
            pass

    return {
        'data': {
            'points': points,
            'errors': errors,
        },
        'meta': {
            'source': 'ENTSOG Transparency Platform',
            'license': 'free, attribution requested',
            'indicator': 'Physical Flow, daily',
            'units': 'kWh/d',
            'discovery': 'auto via /operatorpointdirections',
        },
    }
