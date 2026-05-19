"""
EIA API v2 — Jet Fuel, Distillate, Crude prices + US inventories.

Used as global early-warning signal for jet-fuel supply:
  - US weekly inventory data is published Wednesdays ~10:30 EST
  - Tightening US stocks usually leads EU disruption by 2-4 weeks

Endpoints:
  /petroleum/pri/spt/data   Spot prices (daily/weekly)
  /petroleum/stoc/wstk/data Weekly stocks (PADD district)

Key: free at https://www.eia.gov/opendata/register.php
Set env var: EIA_API_KEY

v5.3 change:
  - _data_rows() now logs EIA's `warnings` and `total` fields when rows is
    empty. EIA returns 200 OK with an empty data array for invalid filter
    combinations (wrong frequency, wrong duoarea code, etc.), making 0-pt
    bugs invisible. The warnings field always names the offending filter.
"""
import os
import time
from typing import Dict, List, Optional

from core import http


BASE = 'https://api.eia.gov/v2'


def _get(path: str, **params) -> dict:
    key = os.environ.get('EIA_API_KEY', '').strip()
    if not key:
        raise RuntimeError('EIA_API_KEY missing')
    full_params = {
        'api_key': key,
        'data[0]': 'value',
        'sort[0][column]': 'period',
        'sort[0][direction]': 'desc',
        'length': 200,
        **params,
    }
    s = http.get_session()
    r = s.get(f'{BASE}/{path}', params=full_params, timeout=30)
    r.raise_for_status()
    return r.json()


def _data_rows(payload: dict, diag_label: str = '') -> List[dict]:
    """
    Extract rows from EIA response. When empty, log EIA's diagnostic info
    so we can see why the filter combination returned nothing.
    """
    resp = payload.get('response', {})
    rows = resp.get('data', []) or []
    if not rows:
        warnings = resp.get('warnings') or []
        total    = resp.get('total')
        # EIA returns 'total' as a string sometimes
        print(f'      EIA {diag_label}: 0 rows (total={total}, warnings={warnings})')
    return rows


def _series_from_rows(rows: List[dict], value_field: str = 'value',
                      period_field: str = 'period') -> List[dict]:
    """Convert EIA response rows to [{date, v}] sorted oldest-first."""
    out = []
    for r in rows:
        p = r.get(period_field)
        v = r.get(value_field)
        if p is None or v is None:
            continue
        try:
            v = float(v)
        except (TypeError, ValueError):
            continue
        out.append({'date': str(p), 'v': v})
    out.sort(key=lambda x: x['date'])
    return out


def fetch() -> dict:
    out: Dict[str, dict] = {}

    # ── Jet Fuel Spot Prices (US Gulf Coast) ──
    # product EPJK = Kerosene-type Jet Fuel; duoarea Y35 = US Gulf Coast
    try:
        d = _get('petroleum/pri/spt/data',
                 frequency='weekly',
                 **{'facets[product][]': 'EPJK',
                    'facets[duoarea][]': 'Y35'})
        out['jet_fuel_us_gulf_weekly'] = {
            'series': _series_from_rows(_data_rows(d, 'jet_fuel_weekly')),
            'unit': 'USD/gal',
            'description': 'Kerosene-type Jet Fuel Spot, US Gulf Coast, weekly',
        }
        print(f'    eia/jet_fuel_us_gulf weekly: {len(out["jet_fuel_us_gulf_weekly"]["series"])} pts')
        time.sleep(0.4)
    except Exception as e:
        print(f'  ! eia/jet_fuel_weekly: {str(e)[:120]}')
        out['jet_fuel_us_gulf_weekly'] = {'series': [], 'unit': 'USD/gal'}

    try:
        d = _get('petroleum/pri/spt/data',
                 frequency='daily',
                 length=500,
                 **{'facets[product][]': 'EPJK',
                    'facets[duoarea][]': 'Y35'})
        out['jet_fuel_us_gulf_daily'] = {
            'series': _series_from_rows(_data_rows(d, 'jet_fuel_daily')),
            'unit': 'USD/gal',
            'description': 'Kerosene-type Jet Fuel Spot, US Gulf Coast, daily',
        }
        print(f'    eia/jet_fuel_us_gulf daily: {len(out["jet_fuel_us_gulf_daily"]["series"])} pts')
        time.sleep(0.4)
    except Exception as e:
        print(f'  ! eia/jet_fuel_daily: {str(e)[:120]}')
        out['jet_fuel_us_gulf_daily'] = {'series': [], 'unit': 'USD/gal'}

    # ── US weekly stocks: Jet Fuel ──
    try:
        d = _get('petroleum/stoc/wstk/data',
                 frequency='weekly',
                 length=200,
                 **{'facets[product][]': 'EPJK',
                    'facets[duoarea][]': 'NUS'})
        out['us_jet_fuel_stocks'] = {
            'series': _series_from_rows(_data_rows(d, 'us_jet_stocks')),
            'unit': 'thousand barrels',
            'description': 'US Total Jet Fuel Stocks, weekly',
        }
        print(f'    eia/us_jet_stocks: {len(out["us_jet_fuel_stocks"]["series"])} pts')
        time.sleep(0.4)
    except Exception as e:
        print(f'  ! eia/us_jet_stocks: {str(e)[:120]}')
        out['us_jet_fuel_stocks'] = {'series': [], 'unit': 'thousand barrels'}

    # ── US weekly stocks: Distillate Fuel Oil (heating oil + diesel proxy) ──
    try:
        d = _get('petroleum/stoc/wstk/data',
                 frequency='weekly',
                 length=200,
                 **{'facets[product][]': 'EPD0',
                    'facets[duoarea][]': 'NUS'})
        out['us_distillate_stocks'] = {
            'series': _series_from_rows(_data_rows(d, 'us_distillate_stocks')),
            'unit': 'thousand barrels',
            'description': 'US Distillate Fuel Oil Stocks, weekly',
        }
        print(f'    eia/us_distillate_stocks: {len(out["us_distillate_stocks"]["series"])} pts')
        time.sleep(0.4)
    except Exception as e:
        print(f'  ! eia/us_distillate_stocks: {str(e)[:120]}')
        out['us_distillate_stocks'] = {'series': [], 'unit': 'thousand barrels'}

    # ── US weekly crude oil stocks ──
    try:
        d = _get('petroleum/stoc/wstk/data',
                 frequency='weekly',
                 length=200,
                 **{'facets[product][]': 'EPC0',
                    'facets[duoarea][]': 'NUS'})
        out['us_crude_stocks'] = {
            'series': _series_from_rows(_data_rows(d, 'us_crude_stocks')),
            'unit': 'thousand barrels',
            'description': 'US Crude Oil Stocks (commercial), weekly',
        }
        print(f'    eia/us_crude_stocks: {len(out["us_crude_stocks"]["series"])} pts')
        time.sleep(0.4)
    except Exception as e:
        print(f'  ! eia/us_crude_stocks: {str(e)[:120]}')
        out['us_crude_stocks'] = {'series': [], 'unit': 'thousand barrels'}

    # ── US weekly gasoline stocks ──
    try:
        d = _get('petroleum/stoc/wstk/data',
                 frequency='weekly',
                 length=200,
                 **{'facets[product][]': 'EPM0',
                    'facets[duoarea][]': 'NUS'})
        out['us_gasoline_stocks'] = {
            'series': _series_from_rows(_data_rows(d, 'us_gasoline_stocks')),
            'unit': 'thousand barrels',
            'description': 'US Finished Motor Gasoline Stocks, weekly',
        }
        print(f'    eia/us_gasoline_stocks: {len(out["us_gasoline_stocks"]["series"])} pts')
        time.sleep(0.4)
    except Exception as e:
        print(f'  ! eia/us_gasoline_stocks: {str(e)[:120]}')
        out['us_gasoline_stocks'] = {'series': [], 'unit': 'thousand barrels'}

    if not any(out[k]['series'] for k in out):
        raise RuntimeError('EIA: all series failed — check EIA_API_KEY')

    return {
        'data': out,
        'meta': {
            'source': 'U.S. Energy Information Administration (EIA APIv2)',
            'license': 'public domain (US Government work)',
            'release_schedule': 'spot prices daily ~5pm EST; weekly stocks Wed ~10:30am EST',
            'note': 'Used as global early-warning signal for jet-fuel supply',
        },
    }
