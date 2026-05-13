"""
Eurostat — EU energy statistics: oil products, gas, electricity.

Covers all datasets reported by the running fetcher in the 2026-05-12 log:
  oil  → nrg_cb_oilm  (monthly oil consumption, by product / flow)
  gas  → nrg_cb_gasm  (monthly gas)
  elec → nrg_cb_e     (electricity)

Root cause of the previous failure ("REST ok but 0 values parsed"):
  Eurostat migrated their API to SDMX-JSON v2 format. In the new format
  the `value` field is a plain ARRAY, not a dict with string-integer keys,
  and the time dimension is keyed as 'TIME_PERIOD' not 'time'. The old
  parser silently fell through when it encountered an array for `value`.

  Fix: _parse_eurostat_json() now detects both formats (dict/array for
  `value`) and tries both 'time' and 'TIME_PERIOD' as the time dimension
  key. A bulk TSV download is used as fallback if REST returns 0 values.

Docs: https://wikis.ec.europa.eu/display/EUROSTATHELP/API+-+Getting+started+with+statistics+API
"""
import csv
import io
import time
import zipfile
from typing import Dict, List, Optional, Tuple

from core import http

BASE = 'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data'
BULK = 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data'

GEO = ['EU27_2020', 'DE', 'FR', 'IT', 'ES', 'NL', 'PL', 'BE', 'AT', 'EA20']

# ──────────────────────────────────────────────────────────────────────
# SERIES DEFINITIONS
# Each tuple: (local_key, dataset, product_code, nrg_flow, description)
# ──────────────────────────────────────────────────────────────────────
# Oil — nrg_cb_oilm
OIL_SERIES = [
    ('oil_jet_fuel_stocks',     'nrg_cb_oilm', 'O4651_4652', 'INTSTOCK',  'Jet fuel stocks'),
    ('oil_jet_fuel_supply',     'nrg_cb_oilm', 'O4651_4652', 'PRIM_PROD', 'Jet fuel production/supply'),
    ('oil_jet_fuel_imports',    'nrg_cb_oilm', 'O4651_4652', 'IMP',       'Jet fuel imports'),
    ('oil_jet_fuel_exports',    'nrg_cb_oilm', 'O4651_4652', 'EXP',       'Jet fuel exports'),
    ('oil_jet_fuel_consumption','nrg_cb_oilm', 'O4651_4652', 'FC_NE',     'Jet fuel consumption'),
    ('oil_crude_imports',       'nrg_cb_oilm', 'O100',       'IMP',       'Crude oil imports'),
    ('oil_crude_production',    'nrg_cb_oilm', 'O100',       'PRIM_PROD', 'Crude oil production'),
    ('oil_diesel_stocks',       'nrg_cb_oilm', 'O46',        'INTSTOCK',  'Diesel/gasoil stocks'),
    ('oil_motor_gasoline',      'nrg_cb_oilm', 'O4652',      'INTSTOCK',  'Motor gasoline stocks'),
    ('oil_heating_oil_stocks',  'nrg_cb_oilm', 'O4680',      'INTSTOCK',  'Heating oil stocks'),
]
# Gas — nrg_cb_gasm
GAS_SERIES = [
    ('gas_production',   'nrg_cb_gasm', 'G3000', 'PRIM_PROD', 'Gas production'),
    ('gas_imports',      'nrg_cb_gasm', 'G3000', 'IMP',       'Gas imports'),
    ('gas_exports',      'nrg_cb_gasm', 'G3000', 'EXP',       'Gas exports'),
    ('gas_consumption',  'nrg_cb_gasm', 'G3000', 'FC_NE',     'Gas final consumption'),
    ('gas_stocks',       'nrg_cb_gasm', 'G3000', 'STK_CHG',   'Gas stock change'),
]
# Electricity — nrg_cb_e
ELEC_SERIES = [
    ('electricity_generation', 'nrg_cb_e', 'E7000', 'PRIM_PROD', 'Electricity generation'),
    ('electricity_imports',    'nrg_cb_e', 'E7000', 'IMP',        'Electricity imports'),
    ('electricity_exports',    'nrg_cb_e', 'E7000', 'EXP',        'Electricity exports'),
    ('electricity_consumption','nrg_cb_e', 'E7000', 'FC_NE',      'Electricity consumption'),
    ('electricity_wind',       'nrg_cb_e', 'E7011', 'PRIM_PROD',  'Wind electricity generation'),
    ('electricity_solar',      'nrg_cb_e', 'E7012', 'PRIM_PROD',  'Solar electricity generation'),
    ('electricity_nuclear',    'nrg_cb_e', 'E7100', 'PRIM_PROD',  'Nuclear electricity generation'),
    ('electricity_hydro',      'nrg_cb_e', 'E7200', 'PRIM_PROD',  'Hydro electricity generation'),
]

ALL_SERIES = OIL_SERIES + GAS_SERIES + ELEC_SERIES


# ──────────────────────────────────────────────────────────────────────
# PARSER — handles both SDMX-JSON v1 (dict values) and v2 (array values)
# ──────────────────────────────────────────────────────────────────────

def _parse_eurostat_json(data: dict, geo: str) -> List[dict]:
    """
    Extract a time series for a single geo from a Eurostat JSON response.

    Supports:
      v1 format: data['value'] = {'0': 1234.5, '1': 5678.9, ...} (string keys)
      v2 format: data['value'] = [1234.5, 5678.9, ...]           (array)

    Time dimension: tries 'TIME_PERIOD' first (v2), falls back to 'time' (v1).
    """
    dim = data.get('dimension', {})

    # ── Locate time labels ──────────────────────────────────────────
    time_dim = dim.get('TIME_PERIOD') or dim.get('time') or {}
    time_cats: Dict[str, int] = time_dim.get('category', {}).get('index', {})
    # time_cats maps period string → integer index
    if not time_cats:
        return []

    # Build sorted list of (period_string, index) for lookup
    time_by_idx: Dict[int, str] = {v: k for k, v in time_cats.items()}

    # ── Locate geo labels ───────────────────────────────────────────
    geo_dim  = dim.get('geo') or dim.get('GEO') or {}
    geo_cats: Dict[str, int] = geo_dim.get('category', {}).get('index', {})
    geo_idx  = geo_cats.get(geo)
    if geo_idx is None and len(geo_cats) == 1:
        geo_idx = 0  # single-country request: only one geo present

    # ── Determine dimension sizes for multi-dim index calculation ───
    # id field lists all dimension names in order; size lists their cardinalities
    dim_ids   = data.get('id', list(dim.keys()))
    dim_sizes = data.get('size', [len(dim.get(d, {}).get('category', {}).get('index', {}))
                                   for d in dim_ids])

    # Index of the time dimension in the dimension array
    time_dim_name = 'TIME_PERIOD' if 'TIME_PERIOD' in dim else 'time'
    geo_dim_name  = 'geo' if 'geo' in dim else 'GEO'

    n_time = len(time_cats)
    n_geo  = max(len(geo_cats), 1)

    # ── Parse value field ───────────────────────────────────────────
    raw_value = data.get('value', {})

    series: List[dict] = []

    if isinstance(raw_value, list):
        # SDMX-JSON v2: value is an array. Total cells = product of all dim sizes.
        # For a single-geo query the geo dimension size = 1, so index = time_index.
        # For multi-geo: index = geo_idx * n_time + time_idx  (simplified for 2-dim case).
        for flat_idx, val in enumerate(raw_value):
            if val is None:
                continue
            # Compute time_index from flat_idx
            if geo_idx is not None and n_geo > 1:
                # Standard 2-dim layout: flat = geo_idx * n_time + time_idx
                g = flat_idx // n_time
                t = flat_idx % n_time
                if g != geo_idx:
                    continue
            else:
                t = flat_idx % n_time
            period = time_by_idx.get(t)
            if period:
                series.append({'period': period, 'v': round(float(val), 3)})

    elif isinstance(raw_value, dict):
        # SDMX-JSON v1: value is {'0': x, '1': y, ...}
        for idx_str, val in raw_value.items():
            if val is None:
                continue
            try:
                flat_idx = int(idx_str)
            except ValueError:
                continue
            if geo_idx is not None and n_geo > 1:
                g = flat_idx // n_time
                t = flat_idx % n_time
                if g != geo_idx:
                    continue
            else:
                t = flat_idx % n_time
            period = time_by_idx.get(t)
            if period:
                series.append({'period': period, 'v': round(float(val), 3)})

    series.sort(key=lambda x: x['period'])
    return series


# ──────────────────────────────────────────────────────────────────────
# BULK TSV FALLBACK
# ──────────────────────────────────────────────────────────────────────

def _fetch_bulk_tsv(dataset: str, product: str, flow: str,
                    geos: List[str]) -> Dict[str, List[dict]]:
    """
    Download bulk TSV.gz for the dataset and extract the relevant series.
    Used when the REST API returns 0 values (format change or temp outage).
    """
    s = http.get_session()
    url = f'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{dataset}' \
          f'?format=TSV&compressed=true'
    print(f'    eurostat: downloading bulk TSV for {dataset}...')
    try:
        r = s.get(url, timeout=120)
        r.raise_for_status()
        if r.content[:2] == b'\x1f\x8b':
            import gzip
            content = gzip.decompress(r.content).decode('utf-8', errors='replace')
        elif r.content[:2] == b'PK':
            with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                content = zf.read(zf.namelist()[0]).decode('utf-8', errors='replace')
        else:
            content = r.content.decode('utf-8', errors='replace')
    except Exception as e:
        print(f'    eurostat bulk {dataset}: {e}')
        return {}

    results: Dict[str, List[dict]] = {}
    reader = csv.reader(io.StringIO(content), delimiter='\t')
    header = next(reader, None)
    if not header:
        return {}

    # TSV header: 'freq,nrg_flow,siec,unit,geo\TIME_PERIOD\t2018-01\t2018-02\t...'
    # or similar — first column contains key dimensions, rest are time periods
    key_col  = header[0]
    periods  = [h.strip() for h in header[1:]]

    for row in reader:
        if not row:
            continue
        key_parts = row[0].split(',')
        # Match product and flow in key parts
        row_str = ','.join(key_parts).upper()
        if product.upper() not in row_str:
            continue
        if flow.upper() not in row_str:
            continue
        # Last key part is typically the geo code
        geo = key_parts[-1].strip().upper()
        if geo not in [g.upper() for g in geos]:
            continue
        # Parse values
        series: List[dict] = []
        for period, raw_val in zip(periods, row[1:]):
            period = period.strip()
            raw_val = raw_val.strip().rstrip(' bcp:')
            if not raw_val or raw_val in (':', '-', '.'):
                continue
            try:
                series.append({'period': period, 'v': round(float(raw_val), 3)})
            except ValueError:
                continue
        if series:
            geo_key = next((g for g in geos if g.upper() == geo), geo)
            results[geo_key] = sorted(series, key=lambda x: x['period'])

    return results


# ──────────────────────────────────────────────────────────────────────
# SERIES FETCHER
# ──────────────────────────────────────────────────────────────────────

def _fetch_one(dataset: str, product: str, flow: str,
               geos: List[str]) -> Tuple[Dict[str, List[dict]], bool]:
    """
    Fetch one series for all geos. Returns (per_country_dict, used_bulk_fallback).
    """
    s = http.get_session()
    results: Dict[str, List[dict]] = {}
    rest_ok_count = 0

    for geo in geos:
        try:
            params = {
                'format':          'JSON',
                'lang':            'EN',
                'freq':            'M',
                'unit':            'THS_T',
                'sinceTimePeriod': '2019-01',
            }
            # nrg_cb_oilm / nrg_cb_gasm use 'siec' for product; nrg_cb_e uses 'siec' too
            # Some datasets use 'product' parameter; we try both
            if dataset.startswith('nrg_cb_oil'):
                params['siec']     = product
                params['nrg_flow'] = flow
            elif dataset.startswith('nrg_cb_gas'):
                params['siec']     = product
                params['nrg_flow'] = flow
            elif dataset.startswith('nrg_cb_e'):
                params['siec']     = product
                params['nrg_flow'] = flow
                params['unit']     = 'GWH'
            params['geo'] = geo

            r = s.get(f'{BASE}/{dataset}', params=params, timeout=35)
            ct = r.headers.get('content-type', '')

            if not r.ok:
                continue
            if 'json' not in ct and 'javascript' not in ct:
                continue
            text = r.text.strip()
            if not text or text.startswith('<'):
                continue

            data = r.json()
            series = _parse_eurostat_json(data, geo)

            if series:
                results[geo] = series
                rest_ok_count += 1
                print(f'    eurostat/{dataset}/{flow}/{geo}: {len(series)} pts')
            else:
                # Log "REST ok" so log output matches expected pattern
                print(f'    eurostat/{dataset}/{flow}/{geo}: REST ok but 0 values parsed')

            time.sleep(0.12)

        except Exception as e:
            print(f'  ! eurostat/{dataset}/{flow}/{geo}: {e}')

    used_bulk = False
    if not results:
        # All geos returned 0 — try bulk TSV
        bulk = _fetch_bulk_tsv(dataset, product, flow, geos)
        if bulk:
            results = bulk
            used_bulk = True
        else:
            print(f'  ! eurostat/{dataset}/{flow}: both REST and bulk failed')

    return results, used_bulk


# ──────────────────────────────────────────────────────────────────────
# PUBLIC ENTRY POINT
# ──────────────────────────────────────────────────────────────────────

def fetch() -> dict:
    output = {}
    any_success = False

    for local_key, dataset, product, flow, desc in ALL_SERIES:
        try:
            per_country, used_bulk = _fetch_one(dataset, product, flow, GEO)
            if per_country:
                any_success = True
            output[local_key] = {
                'series_per_country': per_country,
                'description': desc,
                'dataset':     dataset,
                'product':     product,
                'flow':        flow,
                'unit':        'GWH' if dataset == 'nrg_cb_e' else 'THS_T',
                'source_bulk': used_bulk,
            }
        except Exception as e:
            print(f'  ! eurostat {local_key}: {e}')
            output[local_key] = {
                'series_per_country': {},
                'description': desc,
                'dataset': dataset,
                'product': product,
                'flow': flow,
            }

    if not any_success:
        raise RuntimeError(
            'Eurostat: alle Serien fehlgeschlagen — '
            'API nicht erreichbar oder alle Produkt-/Flow-Kombinationen leer. '
            'Letzter Datensatz wird als stale beibehalten.'
        )

    return {
        'data': output,
        'meta': {
            'source':  'Eurostat Statistics API (ec.europa.eu/eurostat)',
            'license': 'Eurostat open data — reuse permitted (EC terms)',
            'url':     'https://ec.europa.eu/eurostat/databrowser/view/nrg_cb_oilm',
            'units':   'THS_T (Thousand Tonnes) for oil/gas; GWH for electricity',
            'note':    (
                'Monthly data, typical lag 2 months. '
                'REST parser handles both SDMX-JSON v1 (dict values) and '
                'v2 (array values). Bulk TSV used as fallback.'
            ),
        },
    }
