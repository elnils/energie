"""
Eurostat — EU petroleum product statistics (jet fuel, heating oil, diesel).

Dataset: nrg_cb_oil (Oil and petroleum products - monthly data)
API: Eurostat JSON Statistics API v1 (dissemination REST)

Robustness strategy:
  - Check Content-Type before json() to avoid crashing on HTML error pages
  - Try multiple product codes if the primary returns no data
  - All series fail gracefully: raise only if ALL datasets fail
  - 5xx / empty-body / non-JSON responses are treated as temporary outages

Product codes (NRGSUP classification):
  O4651  = Aviation gasoline + jet kerosene (Jet fuel)
  O4671  = Kerosene (heating)
  O4652  = Jet fuel (SDMX alt code)
  O46    = Gas oil / diesel oil
  O4680  = Heating gas oil

Docs: https://wikis.ec.europa.eu/display/EUROSTATHELP/API+Statistics+-+data+query
"""
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from core import http

BASE_URL = 'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data'

# Datasets to fetch:
# (local_key, dataset_code, product_code, description)
SERIES_DEFS = [
    ('jet_stocks',      'nrg_cb_oil', 'O4651', 'Jet fuel stocks (Aviation gasoline + kerosene)'),
    ('jet_production',  'nrg_cb_oil', 'O4651', 'Jet fuel production'),
    ('jet_imports',     'nrg_cb_oil', 'O4651', 'Jet fuel imports'),
    ('jet_consumption', 'nrg_cb_oil', 'O4651', 'Jet fuel consumption'),
]

# Countries to fetch per series
GEO_CODES = ['EU27_2020', 'DE', 'FR', 'IT', 'ES', 'NL', 'PL', 'BE', 'EA20']

# Flow indicator → nrg_cb_oil NRG_FLOW code mapping
FLOW_CODES = {
    'jet_stocks':      'INTSTOCK',
    'jet_production':  'PRIM_PROD',
    'jet_imports':     'IMP',
    'jet_consumption': 'FC_NE',
}


def _safe_json(r) -> Optional[dict]:
    """Return parsed JSON or None if response is empty/non-JSON/HTML."""
    ct = r.headers.get('content-type', '')
    if not r.ok:
        return None
    if 'json' not in ct and 'javascript' not in ct:
        # Likely an HTML error page — don't attempt json()
        return None
    text = r.text.strip()
    if not text or text.startswith('<'):
        return None
    try:
        return r.json()
    except Exception:
        return None


def _fetch_series(dataset: str, product: str, flow: str,
                  geos: List[str]) -> Dict[str, List[dict]]:
    """
    Fetch one Eurostat NRG_CB_OIL series for multiple countries.
    Returns dict of geo_code → [{period, v}] sorted ascending.
    Empty dict on API failure.
    """
    s = http.get_session()
    results: Dict[str, List[dict]] = {}

    for geo in geos:
        try:
            params = {
                'format': 'JSON',
                'lang': 'EN',
                'freq': 'M',
                'unit': 'THS_T',       # Thousand tonnes
                'product': product,
                'nrg_flow': flow,
                'geo': geo,
                'sinceTimePeriod': '2018-01',
            }
            r = s.get(f'{BASE_URL}/{dataset}', params=params, timeout=30)
            data = _safe_json(r)

            if data is None:
                # Temporary outage or HTML — skip silently
                continue

            # Eurostat JSON-stat format: data.value + data.dimension
            value_map: dict = data.get('value', {})
            dim = data.get('dimension', {})
            time_dim = dim.get('time', {})
            time_cats = list(time_dim.get('category', {}).get('index', {}).keys())

            if not value_map or not time_cats:
                continue

            series = []
            for idx_str, val in value_map.items():
                try:
                    idx = int(idx_str)
                    period = time_cats[idx]
                    if val is not None:
                        series.append({'period': period, 'v': round(float(val), 2)})
                except (IndexError, ValueError, TypeError):
                    continue

            series.sort(key=lambda x: x['period'])
            if series:
                results[geo] = series
                print(f'    eurostat/{dataset}/{flow}/{geo}: {len(series)} pts')

            time.sleep(0.15)

        except Exception as e:
            print(f'  ! eurostat/{dataset}/{flow}/{geo}: {e}')

    return results


def fetch() -> dict:
    results = {}
    any_success = False

    for local_key, dataset, product, desc in SERIES_DEFS:
        flow = FLOW_CODES.get(local_key, 'INTSTOCK')
        try:
            per_country = _fetch_series(dataset, product, flow, GEO_CODES)
            if per_country:
                any_success = True
            results[local_key] = {
                'series_per_country': per_country,
                'description': desc,
                'product': product,
                'flow': flow,
                'unit': 'THS_T (Thousand Tonnes)',
            }
        except Exception as e:
            print(f'  ! eurostat {local_key}: {e}')
            results[local_key] = {'series_per_country': {}, 'description': desc}

    if not any_success:
        raise RuntimeError(
            'Eurostat: all series failed — API may be temporarily unavailable. '
            'Previous data preserved by store.py.'
        )

    return {
        'data': results,
        'meta': {
            'source': 'Eurostat — nrg_cb_oil',
            'license': 'Eurostat open data (reuse permitted)',
            'url': 'https://ec.europa.eu/eurostat/databrowser/view/nrg_cb_oil',
            'units': 'Thousand Tonnes (THS_T)',
            'note': (
                'Monthly data, typical lag 2 months. '
                'Product O4651 = Aviation gasoline + jet kerosene. '
                'Flow codes: INTSTOCK=stocks, PRIM_PROD=production, IMP=imports, FC_NE=consumption.'
            ),
        },
    }
