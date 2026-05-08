"""
ENTSOG Transparency Platform — physical gas flows (no auth).

The endpoint is slow (60s per call max). Strategy: pull ONE indicator
(Physical Flow) for a small list of operator point directions on the German
border. We return one daily series per direction.

Reference points (relevant to the current crisis context):
  - Mallnow (DE entry from Nord Stream / Yamal corridor) — historically high,
    now near zero post-2022, kept as visualisation of the change
  - Greifswald — Nord Stream landing, dead but instructive
  - Waidhaus (DE-CZ) — still active, central European supply axis
  - Mediesu Aurit / Brandov — connections to PL/CZ
  - Oberkappel (DE-AT)

If the API is unreachable we let the wrapper preserve previous data.

Docs: https://transparency.entsog.eu/api/archiveDirectories/8/api-manual/
"""
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from core import http


# Curated point directions. operatorKey + pointKey + directionKey form the unique
# tuple. Format: 'pointDirection' parameter combines them as
# '<operator>itp-<point><dir>'. We store the human-readable label too.
POINTS = [
    {'id': 'de-tso-0001itp-00096exit',
     'label': 'Mallnow / DE→PL (GASCADE Yamal)'},
    {'id': 'de-tso-0016itp-00251entry',
     'label': 'Greifswald / NS-1 (Gascade)'},
    {'id': 'de-tso-0017itp-00247entry',
     'label': 'Greifswald / NS-2 (NEL)'},
    {'id': 'cz-tso-0001itp-00010entry',
     'label': 'Waidhaus / DE→CZ (NET4GAS)'},
    {'id': 'de-tso-0001itp-00064exit',
     'label': 'Brandov / DE→CZ (GASCADE)'},
    {'id': 'at-tso-0001itp-00059exit',
     'label': 'Oberkappel / DE→AT (GCA)'},
]


def _iso_day(d: datetime) -> str:
    return d.strftime('%Y-%m-%d')


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
    points: Dict[str, dict] = {}
    errors: List[str] = []
    for p in POINTS:
        try:
            series = _fetch_point(p['id'])
            points[p['id']] = {'label': p['label'], 'series': series}
            last = series[-1] if series else None
            print(f'    entsog/{p["id"][:30]}: {len(series)} pts, last={last["date"] if last else "—"}')
        except Exception as e:
            print(f'  ! entsog/{p["id"][:30]}: {e}')
            errors.append(f'{p["id"]}: {e}')
            points[p['id']] = {'label': p['label'], 'series': []}

    # If literally every point failed, raise so wrapper keeps previous data
    if all(not pt['series'] for pt in points.values()):
        raise RuntimeError(f'ENTSOG: all {len(POINTS)} points returned empty. errors={errors[:3]}')

    return {
        'data': {
            'points': points,
            'errors': errors,
        },
        'meta': {
            'source': 'ENTSOG Transparency Platform (no key required)',
            'license': 'free, attribution requested',
            'indicator': 'Physical Flow, daily',
            'units': 'as reported per point (typically kWh/d)',
        },
    }
