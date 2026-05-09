"""
Tankerkoenig — German fuel station prices.

Reduced footprint vs v4.3:
  - 6 cities (Berlin, Hamburg, München, Frankfurt, Leipzig, Köln)
  - 3 fuel types (E5, E10, Diesel)
  - 2 calls/day cadence (08:00/18:00 Berlin) — set by orchestrator gating

Output schema: per city the avg/min/count for each fuel type, plus a national
average computed from a 100km radius around Germany's geographic centre.

Also writes one daily aggregate row to data/history/fuel.jsonl with the
national averages — used by the frontend for the long-term trend chart.
"""
import os
import time
from typing import Dict, List, Optional

from core import http, validators, history


CITIES: Dict[str, tuple] = {
    'Berlin':    (52.520, 13.405),
    'Hamburg':   (53.550, 10.000),
    'München':   (48.137, 11.575),
    'Frankfurt': (50.110,  8.682),
    'Leipzig':   (51.340, 12.375),
    'Köln':      (50.938,  6.960),
}
NATIONAL_CENTER = (51.163, 10.447)


def _avg(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return round(sum(values) / len(values), 3)


def _fetch_prices(lat: float, lon: float, fuel_type: str, api_key: str,
                  rad: int = 10, retries: int = 4) -> List[float]:
    s = http.get_session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept-Language': 'de-DE,de;q=0.9',
        'Referer': 'https://creativecommons.tankerkoenig.de/',
    }
    last_exc = None
    for attempt in range(retries):
        try:
            r = s.get('https://creativecommons.tankerkoenig.de/json/list.php',
                      params={'lat': lat, 'lng': lon, 'rad': rad,
                              'sort': 'price', 'type': fuel_type, 'apikey': api_key},
                      headers=headers, timeout=25)
            if r.status_code == 503:
                time.sleep(20 + 30 * attempt)
                continue
            r.raise_for_status()
            d = r.json()
            if not d.get('ok'):
                msg = d.get('message', 'unknown')
                # An invalid key gives 'apikey' — bail quickly
                if 'apikey' in msg.lower():
                    raise RuntimeError(f'tankerkoenig rejected key: {msg}')
                raise RuntimeError(f'tankerkoenig: {msg}')
            prices = []
            for st in d.get('stations', []):
                v = st.get('price') or st.get(fuel_type)
                if v and validators.in_range('fuel_eur_l', v):
                    prices.append(round(float(v), 3))
            return sorted(prices)
        except RuntimeError:
            raise
        except Exception as e:
            last_exc = e
            time.sleep(5 + 5 * attempt)
    raise last_exc or RuntimeError('tankerkoenig: all retries exhausted')


def fetch() -> dict:
    api_key = os.environ.get('TANKERKOENIG_API_KEY', '').strip()
    if len(api_key) < 10:
        raise RuntimeError('TANKERKOENIG_API_KEY missing')

    cities_out: Dict[str, dict] = {}
    for city, (lat, lon) in CITIES.items():
        cd = {'count': 0}
        for ft in ('e5', 'e10', 'diesel'):
            try:
                prices = _fetch_prices(lat, lon, ft, api_key, rad=10)
                cd[f'{ft}_avg'] = _avg(prices)
                cd[f'{ft}_min'] = prices[0] if prices else None
                cd[f'{ft}_max'] = prices[-1] if prices else None
                cd['count'] = max(cd['count'], len(prices))
                time.sleep(2.0)
            except Exception as e:
                print(f'      {city}/{ft}: {e}')
                cd[f'{ft}_avg'] = None
                cd[f'{ft}_min'] = None
                time.sleep(5.0)
        cities_out[city] = cd
        print(f'    fuel/{city}: E5={cd.get("e5_avg")} Diesel={cd.get("diesel_avg")}')

    nat = {'count': 0}
    for ft in ('e5', 'e10', 'diesel'):
        try:
            prices = _fetch_prices(NATIONAL_CENTER[0], NATIONAL_CENTER[1], ft, api_key, rad=100)
            nat[f'{ft}_avg'] = _avg(prices)
            nat['count'] = max(nat['count'], len(prices))
            time.sleep(3.0)
        except Exception as e:
            print(f'  ! national/{ft}: {e}')
    cities_out['_national'] = nat

    # Sanity: at least 1 city must have an E5 average
    if not any(v.get('e5_avg') for k, v in cities_out.items() if k != '_national'):
        raise RuntimeError('tankerkoenig: no city returned valid E5 average')

    # History: append national averages so the frontend can plot trends
    history.record_history('fuel', {
        'e5_avg':     nat.get('e5_avg'),
        'e10_avg':    nat.get('e10_avg'),
        'diesel_avg': nat.get('diesel_avg'),
        'n_stations': nat.get('count'),
    })

    return {
        'data': {'cities': cities_out},
        'meta': {
            'source': 'Tankerkoenig API',
            'license': 'CC BY 4.0',
            'units': 'EUR per liter',
            'cadence': '2x per day (gating)',
        },
    }
