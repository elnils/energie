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

v5.4 changes — two bugs that made this fetcher return wrong or no data:

  1. Jet-fuel spot returned 0 rows. The query filtered duoarea=Y35, which is
     a *stock* area code. Spot prices live under duoarea=RGC with process=PF4.
     Both jet-fuel spot series were empty for as long as the file existed.

  2. US crude stocks returned FOUR series stacked on one another — total
     stocks including SPR, SPR alone, commercial, and Cushing — because no
     `process` facet was set. Every date carried 4 values (827246, 3236,
     420261, 406985 for 2025-10-03), so the chart drew a meaningless
     zigzag between unrelated aggregates.

  Both are now pinned by explicit EIA series id (`facets[series][]`), which
  identifies exactly one series and cannot silently widen the way a
  product/area filter can. Each request carries a list of CANDIDATE filter
  sets: the first that returns rows wins, and the winner is recorded in
  meta.resolved_filters so a future EIA rename is visible in the data file
  instead of silently yielding an empty chart.
"""
import os
import time
from typing import Dict, List, Optional, Tuple

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
    """
    Convert EIA response rows to [{date, v}] sorted oldest-first, with one
    value per date.

    The dedupe is deliberate and belongs here rather than only in the
    normalization backstop: if a filter set ever widens again and returns
    several series, we want the count in the log (`N rows -> M dates`) to
    make that obvious rather than average two unrelated aggregates together.
    """
    by_date: Dict[str, float] = {}
    for r in rows:
        p = r.get(period_field)
        v = r.get(value_field)
        if p is None or v is None:
            continue
        try:
            v = float(v)
        except (TypeError, ValueError):
            continue
        by_date[str(p)] = v
    return [{'date': d, 'v': by_date[d]} for d in sorted(by_date)]


def _fetch_candidates(path: str, label: str,
                      candidates: List[Dict],
                      base_params: Optional[Dict] = None
                      ) -> Tuple[List[dict], Optional[Dict]]:
    """
    Try each candidate filter set until one returns rows.

    Returns (series, winning_filters). An empty series with None filters
    means every candidate came back empty — which is a real upstream change,
    not a transient error, so the caller surfaces it rather than retrying.
    """
    base_params = base_params or {}
    for i, filters in enumerate(candidates):
        try:
            payload = _get(path, **base_params, **filters)
            rows = _data_rows(payload, f'{label}#{i}')
            series = _series_from_rows(rows)
            if series:
                if len(rows) != len(series):
                    print(f'    eia/{label}: {len(rows)} rows collapsed to '
                          f'{len(series)} dates — filter may be too wide')
                if i > 0:
                    print(f'    eia/{label}: candidate #{i} used (primary filter empty)')
                return series, filters
        except Exception as e:
            print(f'  ! eia/{label} candidate #{i}: {str(e)[:120]}')
        time.sleep(0.4)
    return [], None


# ──────────────────────────────────────────────────────────────────────
# SERIES DEFINITIONS
# Each entry: (output_key, endpoint, frequency, [candidate filter sets],
#              unit, description)
#
# Candidate order is "most specific first": an explicit EIA series id, then
# the product/area/process triple, then the older loose filter we used to
# send. Keeping the loose one last means a series-id rename degrades to the
# previous behaviour instead of to an empty chart.
# ──────────────────────────────────────────────────────────────────────
SPOT_SERIES = [
    ('jet_fuel_us_gulf_weekly', 'weekly', [
        {'facets[series][]': 'EER_EPJK_PF4_RGC_DPG'},
        {'facets[product][]': 'EPJK', 'facets[duoarea][]': 'RGC',
         'facets[process][]': 'PF4'},
        {'facets[product][]': 'EPJK', 'facets[duoarea][]': 'Y35'},
    ], 'USD/gal', 'Kerosene-type Jet Fuel Spot FOB, US Gulf Coast, weekly'),

    ('jet_fuel_us_gulf_daily', 'daily', [
        {'facets[series][]': 'EER_EPJK_PF4_RGC_DPG'},
        {'facets[product][]': 'EPJK', 'facets[duoarea][]': 'RGC',
         'facets[process][]': 'PF4'},
        {'facets[product][]': 'EPJK', 'facets[duoarea][]': 'Y35'},
    ], 'USD/gal', 'Kerosene-type Jet Fuel Spot FOB, US Gulf Coast, daily'),
]

STOCK_SERIES = [
    ('us_jet_fuel_stocks', [
        {'facets[series][]': 'WKJSTUS1'},
        {'facets[product][]': 'EPJK', 'facets[duoarea][]': 'NUS',
         'facets[process][]': 'SAE'},
        {'facets[product][]': 'EPJK', 'facets[duoarea][]': 'NUS'},
    ], 'US Total Jet Fuel Stocks, weekly'),

    ('us_distillate_stocks', [
        {'facets[series][]': 'WDISTUS1'},
        {'facets[product][]': 'EPD0', 'facets[duoarea][]': 'NUS',
         'facets[process][]': 'SAE'},
        {'facets[product][]': 'EPD0', 'facets[duoarea][]': 'NUS'},
    ], 'US Distillate Fuel Oil Stocks, weekly'),

    # WCESTUS1 = commercial crude EXCLUDING the Strategic Petroleum Reserve.
    # Without the series pin this query also returned total-incl-SPR, SPR
    # alone and Cushing, all on the same dates.
    ('us_crude_stocks', [
        {'facets[series][]': 'WCESTUS1'},
        {'facets[product][]': 'EPC0', 'facets[duoarea][]': 'NUS',
         'facets[process][]': 'SAX'},
        {'facets[product][]': 'EPC0', 'facets[duoarea][]': 'NUS'},
    ], 'US Crude Oil Stocks excl. SPR (commercial), weekly'),

    ('us_gasoline_stocks', [
        {'facets[series][]': 'WGTSTUS1'},
        {'facets[product][]': 'EPM0', 'facets[duoarea][]': 'NUS',
         'facets[process][]': 'SAE'},
        {'facets[product][]': 'EPM0', 'facets[duoarea][]': 'NUS'},
    ], 'US Total Motor Gasoline Stocks, weekly'),
]


def fetch() -> dict:
    out: Dict[str, dict] = {}
    resolved: Dict[str, Optional[Dict]] = {}

    # ── Spot prices ──
    for key, freq, candidates, unit, desc in SPOT_SERIES:
        length = 500 if freq == 'daily' else 200
        series, filters = _fetch_candidates(
            'petroleum/pri/spt/data', key, candidates,
            {'frequency': freq, 'length': length})
        out[key] = {'series': series, 'unit': unit, 'description': desc}
        resolved[key] = filters
        print(f'    eia/{key}: {len(series)} pts')

    # ── Weekly stocks ──
    for key, candidates, desc in STOCK_SERIES:
        series, filters = _fetch_candidates(
            'petroleum/stoc/wstk/data', key, candidates,
            {'frequency': 'weekly', 'length': 200})
        out[key] = {'series': series, 'unit': 'thousand barrels',
                    'description': desc}
        resolved[key] = filters
        print(f'    eia/{key}: {len(series)} pts')

    empty = [k for k, v in out.items() if not v['series']]
    if len(empty) == len(out):
        raise RuntimeError('EIA: all series failed — check EIA_API_KEY')
    if empty:
        # Partial failure is written (the working series are still useful)
        # but named in meta so the dashboard can say which chart is blank
        # and why, instead of showing an unexplained "wird geladen…".
        print(f'  ! eia: {len(empty)} series empty: {empty}')

    return {
        'data': out,
        'meta': {
            'source': 'U.S. Energy Information Administration (EIA APIv2)',
            'license': 'public domain (US Government work)',
            'release_schedule': 'spot prices daily ~5pm EST; weekly stocks Wed ~10:30am EST',
            'note': 'Used as global early-warning signal for jet-fuel supply',
            'empty_series': empty,
            'resolved_filters': {k: v for k, v in resolved.items() if v},
        },
    }
