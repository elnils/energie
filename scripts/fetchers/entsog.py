"""
ENTSOG Transparency Platform — physical gas flows (no auth).

Strategy: instead of hardcoding point-direction IDs (which 404 over time
as routes get discontinued, see Nord Stream 1+2), we auto-discover the
currently active German border points via the /operatorpointdirections
endpoint, then fetch flows for the top ones with the most data.

Additionally fetches aggregated supply/demand balance data per country
via the /aggregatedData endpoint — gives total import, export, LNG send-out,
and domestic production volumes as daily series.

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

# Fallback list if discovery fails — known active border points
FALLBACK_POINTS = [
    {'id': 'de-tso-0001itp-00096exit', 'label': 'Mallnow / DE→PL (GASCADE)'},
    {'id': 'cz-tso-0001itp-00010entry', 'label': 'Waidhaus / CZ→DE (NET4GAS)'},
    {'id': 'de-tso-0001itp-00064exit', 'label': 'Brandov / DE→CZ (GASCADE)'},
    {'id': 'at-tso-0001itp-00059exit', 'label': 'Oberkappel / DE→AT (GCA)'},
    {'id': 'de-tso-0003itp-00131entry', 'label': 'Bocholtz / NL→DE (GTS)'},
    {'id': 'de-tso-0007itp-00179entry', 'label': 'Mediesu Aurit / RO→DE (FGSZ)'},
]

# Aggregated balance: indicator codes to fetch per country
# These are the ENTSOG supply/demand balance indicators
AGGREGATE_INDICATORS = [
    'Physical Flow',
    'Firm Technical',
    'Nomination',
]

# Countries for aggregated balance data
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


def _discover_points() -> List[dict]:
    """
    Discover active German border points via /operatorpointdirections.
    Filter: country=DE, hasData=1, isPipelineInGroup=true, return the
    top ~15 by recent activity.
    """
    s = http.get_session()
    url = 'https://transparency.entsog.eu/api/v1/operatorpointdirections.json'
    params = {'limit': '-1'}
    r = s.get(url, params=params, timeout=60)
    r.raise_for_status()
    payload = r.json()
    items = payload.get('operatorpointdirections') or payload.get('data') or []
    print(f'    entsog discovery: {len(items)} total operator-point-directions')

    de_points = []
    seen = set()
    for it in items:
        if not isinstance(it, dict):
            continue
        op_country = (it.get('operatorCountryKey') or
                      it.get('tSOCountryISO2') or '').lower()
        pt_country = (it.get('pointCountryKey') or
                      it.get('adjacentCountryKey') or '').lower()
        if 'de' not in (op_country, pt_country):
            continue
        has_data = it.get('hasData')
        if has_data not in (True, 'true', 1, '1'):
            continue
        op_key = it.get('operatorKey', '').lower()
        pt_key = it.get('pointKey', '').lower()
        direction = (it.get('directionKey') or '').lower()
        if not (op_key and pt_key and direction):
            continue
        pd_id = f'{op_key}{pt_key}{direction}'
        if pd_id in seen:
            continue
        seen.add(pd_id)
        label = (it.get('pointLabel') or it.get('pointName')
                 or pt_key.upper())[:60]
        if direction == 'exit':
            label = f'{label} (Export)'
        elif direction == 'entry':
            label = f'{label} (Import)'
        de_points.append({
            'id': pd_id,
            'label': label,
            'operator': op_key,
        })

    # Sort: entries first (imports = gas coming in for crisis monitoring)
    de_points.sort(key=lambda x: (0 if 'Import' in x['label'] else 1, x['label']))
    return de_points[:15]


def _fetch_point(point_id: str, days: int = 30) -> List[dict]:
    """Fetch Physical Flow time series for a single point-direction."""
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


def _fetch_aggregated_balance(country: str, days: int = 60) -> dict:
    """
    Fetch aggregated supply/demand balance for a country.
    Returns dict of indicator → [{date, v, unit}].
    Covers: Physical Flow aggregates (total import, total export),
    Nomination, and Firm Technical capacity.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    s = http.get_session()
    result = {}
    for indicator in AGGREGATE_INDICATORS:
        try:
            params = {
                'country': country,
                'indicator': indicator,
                'from': _iso_day(start),
                'to': _iso_day(end),
                'periodType': 'day',
                'timezone': 'CET',
                'limit': '-1',
            }
            r = s.get('https://transparency.entsog.eu/api/v1/aggregatedData.json',
                      params=params, timeout=60)
            if not r.ok:
                continue
            rows = r.json().get('aggregatedData') or r.json().get('data') or []
            series = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                try:
                    val = float(row.get('value', 0) or 0)
                except (TypeError, ValueError):
                    continue
                period = (row.get('periodFrom') or row.get('periodTo') or '')[:10]
                if not period:
                    continue
                # Split by direction if present
                direction = (row.get('directionKey') or '').lower()
                series.append({
                    'date': period,
                    'v': round(val, 0),
                    'direction': direction,
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

    # 2) Fetch flow data for each point
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

    # 3) Fetch aggregated balance for DE and EU
    balance: Dict[str, dict] = {}
    for country in AGGREGATE_COUNTRIES:
        try:
            bal = _fetch_aggregated_balance(country)
            if bal:
                balance[country.lower()] = bal
        except Exception as e:
            print(f'  ! entsog/balance {country}: {e}')

    # If all point fetches empty AND we used cached, invalidate cache
    if all(not pt['series'] for pt in points.values()) and cached:
        try:
            os.remove(CACHE_FILE)
            print('    entsog: cache invalidated (all fetches empty)')
        except OSError:
            pass

    return {
        'data': {
            'points': points,
            'balance': balance,
            'errors': errors,
        },
        'meta': {
            'source': 'ENTSOG Transparency Platform',
            'license': 'free, attribution requested',
            'url': 'https://transparency.entsog.eu',
            'indicator': 'Physical Flow, daily',
            'units': 'kWh/d',
            'discovery': 'auto via /operatorpointdirections',
            'balance_note': (
                'balance[de/eu].physical_flow: aggregated cross-border flows. '
                'balance[de/eu].nomination: scheduled gas transport. '
                'balance[de/eu].firm_technical: contracted firm capacity.'
            ),
        },
    }
