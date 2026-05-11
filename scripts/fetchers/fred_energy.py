"""
FRED API (St. Louis Fed) — simple wrapper for US/global commodity series.

Most series are EIA-rerouted but with a simpler schema and faster updates.
Also adds macro context (Treasury yields, oil OPEC, dollar index).

Key: free at https://fred.stlouisfed.org/docs/api/api_key.html
Set env var: FRED_API_KEY
"""
import os
import time
from typing import Dict, List

from core import http


BASE = 'https://api.stlouisfed.org/fred'


# series_id, label, description, expected_unit
SERIES = [
    # ── Jet fuel ──
    ('DJFUELUSGULF',   'Jet Fuel Spot US Gulf (daily)',   'Kerosene-Type Jet Fuel Prices: U.S. Gulf Coast — daily',  'USD/gal'),
    ('WJFUELUSGULF',   'Jet Fuel Spot US Gulf (weekly)',  'Kerosene-Type Jet Fuel Prices: U.S. Gulf Coast — weekly', 'USD/gal'),
    # ── Diesel / heating oil benchmarks ──
    ('DDFUELUSGULF',   'Diesel Spot US Gulf',            'Ultra-Low-Sulfur No. 2 Diesel — US Gulf Coast',  'USD/gal'),
    ('DHOILNYH',       'Heating Oil NY Harbor',          'No. 2 Heating Oil Prices: New York Harbor',       'USD/gal'),
    # ── Crude ──
    ('DCOILBRENTEU',   'Brent Crude (daily)',            'Crude Oil Prices: Brent - Europe',                'USD/barrel'),
    ('DCOILWTICO',     'WTI Crude (daily)',              'Crude Oil Prices: West Texas Intermediate',       'USD/barrel'),
    # ── Macro context ──
    ('DTB3',           '3-Month T-Bill',                 '3-Month Treasury Bill: Secondary Market Rate',    '%'),
    ('DGS10',          '10-Year Treasury',               '10-Year Treasury Constant Maturity Rate',         '%'),
    ('DTWEXBGS',       'USD Trade Weighted Index',       'Trade Weighted U.S. Dollar Index: Broad',         'index'),
    ('VIXCLS',         'VIX (Volatility)',               'CBOE Volatility Index: VIX',                      'index'),
]


def _get_series(series_id: str, start: str = '2020-01-01') -> List[dict]:
    key = os.environ.get('FRED_API_KEY', '').strip()
    if not key:
        raise RuntimeError('FRED_API_KEY missing')
    s = http.get_session()
    r = s.get(f'{BASE}/series/observations', params={
        'series_id': series_id,
        'api_key': key,
        'file_type': 'json',
        'observation_start': start,
    }, timeout=30)
    r.raise_for_status()
    obs = r.json().get('observations', [])
    out = []
    for o in obs:
        if o.get('value') in (None, '.', ''):
            continue
        try:
            v = float(o['value'])
        except ValueError:
            continue
        out.append({'date': o['date'], 'v': v})
    out.sort(key=lambda x: x['date'])
    return out


def fetch() -> dict:
    out: Dict[str, dict] = {}
    success_count = 0
    for series_id, label, description, unit in SERIES:
        try:
            data = _get_series(series_id)
            out[series_id] = {
                'label': label,
                'description': description,
                'unit': unit,
                'series': data,
            }
            if data:
                success_count += 1
                last = data[-1]
                print(f'    fred/{series_id}: {len(data)} pts, last={last["date"]}={last["v"]}')
            else:
                print(f'    fred/{series_id}: empty')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! fred/{series_id}: {str(e)[:120]}')
            out[series_id] = {
                'label': label, 'description': description, 'unit': unit, 'series': [],
            }

    if success_count == 0:
        raise RuntimeError('FRED: all series failed — check FRED_API_KEY')

    return {
        'data': out,
        'meta': {
            'source': 'FRED (Federal Reserve Bank of St. Louis)',
            'license': 'data is in the public domain',
            'note': 'EIA petroleum series rerouted via FRED for simpler schema',
            'series_total': len(SERIES),
            'series_with_data': success_count,
        },
    }
