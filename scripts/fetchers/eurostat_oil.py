"""
Eurostat — EU petroleum supply, including jet fuel stocks and imports.

The only public source for absolute EU/national-level stock levels.
Schedule: monthly with ~2-month lag (March data published mid-May).

Datasets used:
  nrg_cb_oilm — Supply and transformation of oil and petroleum products (monthly)
  nrg_ti_oilm — Imports of oil and petroleum products by partner country

SDMX 2.1 REST API, no key needed.
Returns JSON-stat format which we flatten to time-series.
"""
import time
from typing import Dict, List, Optional

from core import http


BASE = 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data'


# Eurostat product codes (siec dimension)
# O4661XR5230B = Kerosene-type jet fuel
# O4671 = Gasoline aviation
# O4630 = Motor gasoline
# O4671XR5220B = Aviation gasoline
PRODUCTS = {
    'jet_kerosene': 'O4661XR5230B',
}

# Energy balance positions (nrg_bal dimension)
# PRD_C = production
# IMP = imports
# EXP = exports
# STK_C = closing stocks (current month-end)
# FC_T = final consumption total
BALANCES = {
    'production': 'PRD_C',
    'imports': 'IMP',
    'stocks': 'STK_C',
    'consumption': 'FC_T',
}

# EU geo codes worth tracking
GEO_CODES = ['EU27_2020', 'EA20', 'DE', 'FR', 'IT', 'ES', 'NL', 'PL', 'BE']


def _fetch_dataset(dataset: str, **filters) -> dict:
    """Fetch a Eurostat dataset and return JSON-stat dict."""
    s = http.get_session()
    params = {'format': 'JSON', **filters}
    r = s.get(f'{BASE}/{dataset}', params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def _flatten_jsonstat(payload: dict) -> List[dict]:
    """
    JSON-stat is a multidimensional cube. We flatten to flat rows.
    Each row has: geo, time (period), value, plus any other dims.
    """
    if 'value' not in payload or 'dimension' not in payload:
        return []
    values = payload['value']           # dict: index_str -> value
    dim_order = payload.get('id', [])
    sizes = payload.get('size', [])
    dimensions = payload['dimension']

    # Build per-dim index->code lookup
    dim_code_lookups = {}
    for dim_name in dim_order:
        cats = dimensions.get(dim_name, {}).get('category', {})
        index_map = cats.get('index', {})
        # JSON-stat 'index' can be dict {code: pos} OR list [code, code, ...]
        if isinstance(index_map, dict):
            inverse = {v: k for k, v in index_map.items()}
        elif isinstance(index_map, list):
            inverse = {i: code for i, code in enumerate(index_map)}
        else:
            inverse = {}
        dim_code_lookups[dim_name] = inverse

    rows = []
    total_cells = 1
    for sz in sizes:
        total_cells *= sz

    # Iterate every cell that has a value
    for idx_str, val in values.items():
        try:
            idx_flat = int(idx_str)
        except (ValueError, TypeError):
            continue
        if val is None:
            continue
        # Decompose flat index to multi-dim coordinates
        coords = []
        rem = idx_flat
        for sz in reversed(sizes):
            coords.append(rem % sz)
            rem //= sz
        coords.reverse()
        # Map coords back to dimension codes
        row = {'value': float(val)}
        for dim_name, pos in zip(dim_order, coords):
            code = dim_code_lookups.get(dim_name, {}).get(pos, str(pos))
            row[dim_name] = code
        rows.append(row)
    return rows


def _rows_to_series(rows: List[dict], geo: str) -> List[dict]:
    """Filter rows to one geography, return [{period, v}] sorted oldest-first."""
    out = []
    for r in rows:
        if r.get('geo') != geo:
            continue
        period = r.get('time')
        if period is None:
            continue
        out.append({'period': str(period), 'v': r['value']})
    out.sort(key=lambda x: x['period'])
    return out


def fetch() -> dict:
    out: Dict[str, dict] = {}

    # ── Jet fuel: production, imports, stocks per country ──
    for balance_name, balance_code in BALANCES.items():
        try:
            payload = _fetch_dataset(
                'nrg_cb_oilm',
                siec=PRODUCTS['jet_kerosene'],
                nrg_bal=balance_code,
                unit='THS_T',          # thousand tonnes
                startPeriod='2020-01',
            )
            rows = _flatten_jsonstat(payload)
            per_geo: Dict[str, List[dict]] = {}
            for geo in GEO_CODES:
                series = _rows_to_series(rows, geo)
                if series:
                    per_geo[geo] = series
            out[f'jet_{balance_name}'] = {
                'unit': 'thousand tonnes',
                'description': f'Jet kerosene {balance_name} (monthly)',
                'series_per_country': per_geo,
            }
            print(f'    eurostat jet/{balance_name}: '
                  f'{sum(len(v) for v in per_geo.values())} total points '
                  f'across {len(per_geo)} countries')
            time.sleep(0.8)
        except Exception as e:
            print(f'  ! eurostat jet/{balance_name}: {str(e)[:120]}')
            out[f'jet_{balance_name}'] = {
                'unit': 'thousand tonnes',
                'description': f'Jet kerosene {balance_name} (monthly)',
                'series_per_country': {},
            }

    # Sanity: at least one series populated
    has_any = any(out[k].get('series_per_country') for k in out)
    if not has_any:
        raise RuntimeError('Eurostat: all series failed')

    return {
        'data': out,
        'meta': {
            'source': 'Eurostat — Supply, transformation and consumption of energy',
            'datasets': ['nrg_cb_oilm'],
            'license': 'Eurostat re-use policy (free with attribution)',
            'lag': 'monthly, typically published with 2-month delay',
            'note': 'Only public source for absolute EU jet fuel stock levels',
        },
    }
