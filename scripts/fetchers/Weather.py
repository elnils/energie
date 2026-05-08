"""
Open-Meteo — temperature, wind 100m, solar radiation.

8 cities, 5-day forecast, hourly resolution.
"""
import time
from typing import Dict

from core import http


CITIES = {
    'Berlin':    (52.52, 13.41),
    'Hamburg':   (53.55, 10.00),
    'München':   (48.14, 11.58),
    'Frankfurt': (50.11,  8.68),
    'Köln':      (50.94,  6.96),
    'Stuttgart': (48.78,  9.18),
    'Düsseldorf':(51.22,  6.77),
    'Leipzig':   (51.34, 12.38),
}


def fetch() -> dict:
    s = http.get_session()
    out: Dict[str, dict] = {}
    for city, (lat, lon) in CITIES.items():
        try:
            r = s.get('https://api.open-meteo.com/v1/forecast', params={
                'latitude': lat, 'longitude': lon,
                'current': 'temperature_2m,wind_speed_10m,direct_radiation,relative_humidity_2m',
                'hourly': 'temperature_2m,wind_speed_100m,direct_radiation,cloud_cover,precipitation_probability',
                'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum,'
                         'wind_speed_10m_max,shortwave_radiation_sum',
                'forecast_days': 5,
                'timezone': 'Europe/Berlin',
            }, timeout=15)
            r.raise_for_status()
            out[city] = r.json()
            time.sleep(0.07)
        except Exception as e:
            print(f'  ! weather/{city}: {e}')

    if not out:
        raise RuntimeError('Open-Meteo: zero cities returned data')

    return {
        'data': {'cities': out},
        'meta': {
            'source': 'Open-Meteo',
            'license': 'CC BY 4.0',
        },
    }
