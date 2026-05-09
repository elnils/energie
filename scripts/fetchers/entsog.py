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


# Curated point directions on the German gas-transmission border.
# These IDs follow the format <country>-<tso>-<num>itp-<num><direction>
# Verified against ENTSOG's own R library examples (krose/entsog).
# When a point is 404 / dead, the fetcher logs it in errors[] and the
# frontend skips empty series. We over-provision so a few dead ones are OK.
POINTS = [
    # Active corridors (verified working)
    {'id': 'de-tso-0001itp-00096exit',
     'label': 'Mallnow / DE→PL (GASCADE Yamal)'},
    # Nord Stream 1+2 — kept as historical visualisation, currently zero
    {'id': 'de-tso-0017itp-00247entry',
     'label': 'Greifswald NS-2 (NEL, dead)'},
    # Likely active points — try multiple, fetcher reports which work
    {'id': 'de-tso-0002itp-00080entry',
     'label': 'Emden EUROPIPE (OGE entry NO)'},
    {'id': 'de-tso-0001itp-00012entry',
     'label': 'Lubmin entry (GASCADE)'},
    {'id': 'de-tso-0009itp-00216exit',
     'label': 'OPAL (GASCADE→CZ)'},
    {'id': 'de-tso-0002itp-00079exit',
     'label': 'Bocholtz / DE→NL (OGE)'},
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
