"""
Shared Eurostat dissemination-API client.

Why this module exists
----------------------
The Eurostat fetcher had been asking for dimension codes that do not exist.
Eurostat answers such a request with HTTP 200 and an empty cube, so every
broken series looked exactly like a temporarily unavailable one:

    oil_jet_fuel_stocks      siec=O4651_4652   nrg_bal=INTSTOCK    -> 0 rows
    gas_production           siec=G3000        nrg_bal=PRIM_PROD   -> 0 rows
    electricity_generation   siec=E7000        nrg_bal=PRIM_PROD   -> 0 rows

while the three series that happened to use real codes (IMP, EXP, STK_CHG)
returned data. Ten of twenty-three series in eurostat_oil.json were empty
for this reason, including every one the Treibstoff tab draws.

Hard-coding a corrected code list would fix today and break again at the
next Eurostat vocabulary revision, with the same silent signature. So this
client asks the API what codes a dataset actually has, and matches them by
their human-readable label:

    describe('nrg_cb_oilm')  ->  {'siec': {'O4661XR5230B': 'Kerosene-type
                                            jet fuel (excluding biofuel
                                            portion)', ...},
                                  'nrg_bal': {'IMP': 'Imports', ...}, ...}

A series is then declared by intent ("jet fuel", "imports") rather than by
code, and `resolve()` reports which code each pattern matched. Those go into
the data file's meta, so a rename is visible as a changed code instead of an
empty chart.
"""
import re
import time
from typing import Dict, List, Optional, Tuple

from core import http

BASE = 'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data'

# Cache describe() per dataset for the lifetime of one fetch run. Each call
# is a real HTTP request and several series share a dataset.
_DESCRIBE_CACHE: Dict[str, Dict[str, Dict[str, str]]] = {}


def _get(dataset: str, params: Dict, timeout: int = 40) -> dict:
    s = http.get_session()
    r = s.get(f'{BASE}/{dataset}', params={'format': 'JSON', 'lang': 'EN', **params},
              timeout=timeout)
    if not r.ok:
        raise RuntimeError(f'HTTP {r.status_code}: {(r.text or "")[:200]}')
    ct = r.headers.get('content-type', '')
    if 'json' not in ct and 'javascript' not in ct:
        raise RuntimeError(f'non-JSON response ({ct}): {(r.text or "")[:160]}')
    return r.json()


def describe(dataset: str, probe_geo: str = 'DE') -> Dict[str, Dict[str, str]]:
    """
    Return {dimension: {code: label}} for a dataset.

    Implemented as a minimal data request (one country, one period) rather
    than a metadata call: the JSON-stat response carries the full category
    list for every dimension regardless of how narrow the selection is, so
    this costs one small request and needs no second API surface.
    """
    if dataset in _DESCRIBE_CACHE:
        return _DESCRIBE_CACHE[dataset]
    out: Dict[str, Dict[str, str]] = {}
    try:
        payload = _get(dataset, {'geo': probe_geo, 'lastTimePeriod': 1})
        for dim_name, dim in (payload.get('dimension') or {}).items():
            cats = (dim.get('category') or {})
            index = cats.get('index') or {}
            labels = cats.get('label') or {}
            if isinstance(index, dict):
                codes = list(index.keys())
            elif isinstance(index, list):
                codes = list(index)
            else:
                codes = []
            out[dim_name] = {c: str(labels.get(c, c)) for c in codes}
        print(f'    eurostat/{dataset}: dimensions ' +
              ', '.join(f'{k}({len(v)})' for k, v in out.items()))
    except Exception as e:
        print(f'  ! eurostat/{dataset} describe: {str(e)[:160]}')
    _DESCRIBE_CACHE[dataset] = out
    return out


def resolve(catalog: Dict[str, str],
            preferred_codes: Tuple[str, ...] = (),
            label_patterns: Tuple[str, ...] = (),
            exclude_patterns: Tuple[str, ...] = ()) -> Optional[str]:
    """
    Pick one code out of a dimension's catalog.

    Order: an explicit code that the dataset really offers wins; otherwise
    the first code whose label matches one of `label_patterns` (evaluated in
    order, so the caller controls precedence) and none of `exclude_patterns`.

    Returns None when the dimension has nothing suitable — the caller then
    reports the series as unavailable for this dataset rather than sending a
    request that would come back empty.
    """
    if not catalog:
        # No catalog (describe failed) — fall back to the caller's first
        # explicit guess so a transient metadata failure doesn't disable
        # an otherwise working series.
        return preferred_codes[0] if preferred_codes else None
    for code in preferred_codes:
        if code in catalog:
            return code
    for pattern in label_patterns:
        rx = re.compile(pattern, re.IGNORECASE)
        for code, label in catalog.items():
            if not rx.search(label):
                continue
            if any(re.search(x, label, re.IGNORECASE) for x in exclude_patterns):
                continue
            return code
    return None


def parse_series(payload: dict) -> List[dict]:
    """
    Extract [{period, v}] from a JSON-stat response whose non-time dimensions
    are all filtered to a single value.

    Eurostat orders the value array with TIME_PERIOD last, so with every
    other dimension pinned to one category the flat index IS the time index.
    Both response shapes are handled: `value` as a sparse dict keyed by
    stringified index (observations with no data are simply absent), and as
    a dense array.
    """
    dim = payload.get('dimension') or {}
    time_dim = dim.get('TIME_PERIOD') or dim.get('time') or {}
    index = ((time_dim.get('category') or {}).get('index') or {})
    if isinstance(index, list):
        index = {code: i for i, code in enumerate(index)}
    if not index:
        return []
    by_idx = {i: period for period, i in index.items()}
    n_time = len(by_idx)

    raw = payload.get('value')
    points: Dict[str, float] = {}
    if isinstance(raw, dict):
        items = raw.items()
    elif isinstance(raw, list):
        items = ((str(i), v) for i, v in enumerate(raw))
    else:
        return []
    for key, val in items:
        if val is None:
            continue
        try:
            flat = int(key)
        except (TypeError, ValueError):
            continue
        period = by_idx.get(flat % n_time)
        if period is None:
            continue
        try:
            points[period] = round(float(val), 3)
        except (TypeError, ValueError):
            continue
    return [{'period': p, 'v': points[p]} for p in sorted(points)]


def fetch_per_country(dataset: str, filters: Dict[str, str], geos: List[str],
                      since: Optional[str] = None,
                      pause: float = 0.12) -> Dict[str, List[dict]]:
    """
    Run one request per geo with all non-time dimensions pinned.

    One country at a time, deliberately: it keeps `parse_series`'s index
    arithmetic valid without having to reimplement JSON-stat's multi-
    dimensional layout, and a single country failing then costs only its own
    series rather than the whole set.
    """
    results: Dict[str, List[dict]] = {}
    for geo in geos:
        params = dict(filters)
        params['geo'] = geo
        if since:
            params['sinceTimePeriod'] = since
        try:
            series = parse_series(_get(dataset, params))
            if series:
                results[geo] = series
        except Exception as e:
            print(f'  ! eurostat/{dataset}/{geo}: {str(e)[:120]}')
        time.sleep(pause)
    return results
