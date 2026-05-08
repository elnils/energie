"""
Three-layer validation for fetched values.

Layer 1 — RANGE: hard min/max sanity check per source.
Layer 2 — JUMP: max relative change vs previous value.
Layer 3 — FRESHNESS: max age of latest data point.

The validator is *advisory*: callers decide whether to drop, quarantine, or
keep. Default policy used by fetchers: range failure -> drop value (keep last
good), jump failure -> keep new but flag, freshness failure -> mark stale.

All thresholds live in ONE dict so they're auditable in code review.
"""
from typing import Any, Dict, List, Optional, Tuple

# (min, max) hard bounds per metric. None = no bound on that side.
RANGES: Dict[str, Tuple[Optional[float], Optional[float]]] = {
    'price_da_eur_mwh':       (-500.0, 4000.0),   # negative day-ahead is real
    'gas_ttf_eur_mwh':        (0.0,    500.0),
    'co2_eua_eur_t':          (0.0,    250.0),
    'gas_storage_pct':        (0.0,    100.0),
    'lng_storage_pct':        (0.0,    100.0),
    'fx_eur_usd':             (0.5,    2.0),
    'fx_eur_gbp':             (0.4,    1.5),
    'brent_usd_bbl':          (10.0,   300.0),
    'wti_usd_bbl':            (10.0,   300.0),
    'henryhub_usd_mmbtu':     (0.5,    50.0),
    'gold_usd_oz':            (500.0,  10000.0),
    'fuel_eur_l':             (0.5,    5.0),
    'heating_oil_eur_l':      (0.3,    5.0),
    'gen_mw':                 (0.0,    150000.0),
    'temperature_c':          (-50.0,  60.0),
    'wind_kmh':               (0.0,    300.0),
    'radiation_wm2':          (0.0,    1500.0),
    'vpi_index':              (50.0,   500.0),
}

# Max tolerated relative jump in 12h, per metric. Day-ahead can really swing.
MAX_JUMP_PCT: Dict[str, float] = {
    'price_da_eur_mwh':       300.0,
    'gas_ttf_eur_mwh':        40.0,
    'co2_eua_eur_t':          25.0,
    'gas_storage_pct':        15.0,   # absolute pp also works since fill is %
    'fx_eur_usd':             5.0,
    'fx_eur_gbp':             5.0,
    'brent_usd_bbl':          30.0,
    'wti_usd_bbl':            30.0,
    'henryhub_usd_mmbtu':     50.0,
    'gold_usd_oz':            15.0,
    'fuel_eur_l':             20.0,
    'heating_oil_eur_l':      25.0,
}

# Max age in seconds before data point is considered stale.
MAX_AGE_SEC: Dict[str, int] = {
    'smard':            6 * 3600,        # 6h
    'energy_charts':    24 * 3600,       # 1d (price publication once a day)
    'gas_storage':      48 * 3600,       # 2d
    'lng_storage':      48 * 3600,
    'fuel':             18 * 3600,       # 18h (we fetch 2x/day)
    'heating_oil':      36 * 3600,
    'commodities':      4 * 24 * 3600,   # 4d (Yahoo can lag on weekends)
    'fx':               4 * 24 * 3600,
    'weather':          3 * 3600,
    'destatis_vpi':     45 * 24 * 3600,  # monthly publication
    'entsog':           48 * 3600,
    'news':             7 * 24 * 3600,
}


def in_range(metric: str, value: Any) -> bool:
    """Return True if value is within configured range (or no range set)."""
    if value is None:
        return False
    try:
        v = float(value)
    except (TypeError, ValueError):
        return False
    bounds = RANGES.get(metric)
    if bounds is None:
        return True
    lo, hi = bounds
    if lo is not None and v < lo:
        return False
    if hi is not None and v > hi:
        return False
    return True


def jump_ok(metric: str, prev: Any, current: Any) -> bool:
    """Return True if relative change between prev and current is within bounds."""
    if prev is None or current is None:
        return True
    try:
        p, c = float(prev), float(current)
    except (TypeError, ValueError):
        return True
    if p == 0:
        return True
    pct = abs(c - p) / abs(p) * 100.0
    threshold = MAX_JUMP_PCT.get(metric)
    if threshold is None:
        return True
    return pct <= threshold


def is_fresh(source: str, latest_epoch: Optional[int], now_epoch: int) -> bool:
    """Return True if the latest data point is within the freshness window."""
    if latest_epoch is None:
        return False
    max_age = MAX_AGE_SEC.get(source)
    if max_age is None:
        return True
    return (now_epoch - latest_epoch) <= max_age


def filter_series(metric: str, series: List[dict], value_key: str = 'v') -> Tuple[List[dict], List[dict]]:
    """
    Split series into (clean, rejected) by range check on `value_key`.
    Series elements are dicts; non-conforming ones go to rejected with a `_reason`.
    """
    clean: List[dict] = []
    rejected: List[dict] = []
    for entry in series:
        if not isinstance(entry, dict):
            continue
        v = entry.get(value_key)
        if in_range(metric, v):
            clean.append(entry)
        else:
            rejected.append({**entry, '_reason': f'out_of_range[{metric}]'})
    return clean, rejected


def validate_value(metric: str, value: Any, prev: Any = None) -> Tuple[bool, Optional[str]]:
    """Combined check. Returns (ok, reason_if_not)."""
    if not in_range(metric, value):
        return False, f'out_of_range[{metric}]'
    if prev is not None and not jump_ok(metric, prev, value):
        return False, f'jump_exceeded[{metric}]'
    return True, None
