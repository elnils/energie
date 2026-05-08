"""
Energy-Charts (api.energy-charts.info, Fraunhofer ISE).

Pulls:
  - Day-ahead prices for DE-LU, AT, FR, PL, CH (last 7 days)
  - Monthly avg DE prices since 2005 (long history; combines DE-AT-LU and DE-LU)
  - Public power generation last 7 days
  - Renewable share last 30 days
  - Installed capacity per year
  - TTF gas price (EUR/MWh, 2 years)
  - EU ETS CO2 price (EUR/t, 2 years)
"""
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List

from core import http


BASE = 'https://api.energy-charts.info'


def _get(path: str, params: dict | None = None) -> dict:
    s = http.get_session()
    r = s.get(f'{BASE}/{path}', params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def _first_list(d: dict, *keys) -> list:
    for k in keys:
        v = d.get(k)
        if isinstance(v, list) and v:
            return v
    return []


def fetch() -> dict:
    out: Dict[str, dict] = {}
    end = datetime.now(timezone.utc)
    end_str = end.strftime('%Y-%m-%d')

    # 7-day day-ahead per bidding zone
    for bzn, key in [('DE-LU', 'price_de'), ('AT', 'price_at'),
                     ('FR', 'price_fr'), ('PL', 'price_pl'), ('CH', 'price_ch')]:
        try:
            d = _get('price', {'bzn': bzn, 'start': 'P7D'})
            out[key] = {
                'unix_seconds': d.get('unix_seconds', []),
                'price': d.get('price', []),
                'unit': 'EUR/MWh',
            }
            time.sleep(0.15)
        except Exception as e:
            print(f'  ! ec/{key}: {e}')
            out[key] = {'unix_seconds': [], 'price': [], 'unit': 'EUR/MWh'}

    # Monthly long history DE
    try:
        new = _get('price', {'bzn': 'DE-LU', 'start': '2018-10-01',
                             'end': end_str, 'interval': 'month'})
        old = _get('price', {'bzn': 'DE-AT-LU', 'start': '2005-01-01',
                             'end': '2018-09-30', 'interval': 'month'})
        out['price_de_monthly'] = {
            'unix_seconds': old.get('unix_seconds', []) + new.get('unix_seconds', []),
            'price': old.get('price', []) + new.get('price', []),
            'unit': 'EUR/MWh',
        }
        time.sleep(0.2)
    except Exception as e:
        print(f'  ! ec/monthly: {e}')
        out['price_de_monthly'] = {'unix_seconds': [], 'price': []}

    # Public power 7 days
    try:
        out['public_power_de'] = _get('public_power', {
            'country': 'de',
            'start': (end - timedelta(days=7)).strftime('%Y-%m-%dT%H:%MZ'),
            'end': end.strftime('%Y-%m-%dT%H:%MZ'),
        })
        time.sleep(0.2)
    except Exception as e:
        print(f'  ! ec/public_power: {e}')

    # Renewable share 30d
    try:
        d = _get('ren_share_in_public_power', {'country': 'de', 'start': 'P30D'})
        out['ren_share_de'] = {
            'unix_seconds': d.get('unix_seconds', []),
            'ren_share': _first_list(d, 'share_of_generation_capacity', 'ren_share',
                                     'renewable_share', 'ren_share_in_public_power'),
        }
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/ren_share: {e}')
        out['ren_share_de'] = {'unix_seconds': [], 'ren_share': []}

    # Installed power yearly
    try:
        out['installed_de'] = _get('installed_power', {'country': 'de', 'time_step': 'yearly'})
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/installed: {e}')

    # TTF gas
    try:
        d = _get('gas_price', {'start': 'P730D'})
        out['gas_price'] = {
            'unix_seconds': _first_list(d, 'unix_seconds', 'timestamp', 'time'),
            'price': _first_list(d, 'Gas Price', 'price', 'gas_price', 'value', 'data'),
            'unit': 'EUR/MWh',
        }
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/gas_price: {e}')
        out['gas_price'] = {'unix_seconds': [], 'price': []}

    # CO2
    try:
        d = _get('co2_price', {'start': 'P730D'})
        out['co2_price'] = {
            'unix_seconds': _first_list(d, 'unix_seconds', 'timestamp', 'time'),
            'price': _first_list(d, 'CO2 Price', 'co2_price', 'price', 'value', 'data'),
            'unit': 'EUR/tCO2',
        }
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/co2_price: {e}')
        out['co2_price'] = {'unix_seconds': [], 'price': []}

    return {
        'data': out,
        'meta': {
            'source': 'Energy-Charts (Fraunhofer ISE)',
            'license': 'CC BY 4.0',
        },
    }
