"""
Series normalization — the single guarantee that no source emits duplicates.

Why this exists:
    Several upstreams publish the same observation more than once:

      - ENTSO-E republishes a day in both PT60M and PT15M resolution, so a
        naive flatten of all TimeSeries produced two values for the same
        timestamp (DE_LU: 1444 points for 767 distinct timestamps).
      - Destatis ffcsv carries one row per (category x measure), so an index
        value and its year-on-year change rate both landed under the same
        `period`, drawing a sawtooth line in the VPI chart.
      - Any fetcher that pages over overlapping windows can repeat points.

    Rather than patch each fetcher and hope the next upstream change is
    caught, `normalize_payload()` walks the whole `data` dict of EVERY source
    before it is written and enforces the invariant structurally:

        a list of observation dicts has exactly one entry per time key,
        sorted ascending.

    Fetchers may still dedupe themselves when they have extra knowledge
    (e.g. ENTSO-E preferring the finer resolution). This layer is the
    backstop that holds when they don't.

Time keys recognised, in priority order: 'ts', 'period', 'date', 'time'.
A list only qualifies if the majority of its entries are dicts carrying the
same time key — so lists of countries, articles or config rows are untouched.
"""
from typing import Any, Dict, List, Optional, Tuple

TIME_KEYS = ('ts', 'period', 'date', 'time')

# Keys that mark a dict as an observation rather than a record we must not
# collapse. A news article has a 'date' but also a 'title'/'link' — collapsing
# those would delete content, so we require a value-ish key to be present.
VALUE_KEYS = ('v', 'value', 'price', 'fill_pct', 'pct', 'avg', 'min', 'max',
              'injection', 'withdrawal', 'trend', 'gas_in_storage_twh')

# Metadata that is constant for a series and therefore never distinguishes
# two entries from one another.
IGNORED_KEYS = ('unit', 'type', '_period', '_n')


def _time_key(sample: dict) -> Optional[str]:
    for k in TIME_KEYS:
        if k in sample:
            return k
    return None


def _discriminators(entry: dict, time_key: str) -> dict:
    """Fields that identify WHICH series an entry belongs to, if any."""
    return {k: v for k, v in entry.items()
            if k != time_key and k not in VALUE_KEYS and k not in IGNORED_KEYS}


def _looks_like_observations(items: List[Any]) -> Optional[str]:
    """
    Return the time key if `items` is a plain single series, else None.

    Requires, in order:
      1. at least 2 entries, all dicts, sharing a time key;
      2. at least one recognised value key (keeps news articles out);
      3. no *discriminating* field that differs between entries sharing a
         timestamp.

    Rule 3 is what protects genuinely multi-valued lists. ENTSOG's balance
    series carries 341 rows per date, one per entry/exit point, separated by
    a `direction` field — those are not duplicates and collapsing them would
    delete most of the file. A true duplicate has nothing to tell it apart.
    """
    if len(items) < 2:
        return None
    if not all(isinstance(x, dict) for x in items):
        return None
    tk = _time_key(items[0])
    if tk is None:
        return None
    if not all(tk in x for x in items):
        return None
    if not any(any(vk in x for vk in VALUE_KEYS) for x in items):
        return None

    by_time: Dict[Any, List[dict]] = {}
    for x in items:
        by_time.setdefault(x.get(tk), []).append(x)
    for group in by_time.values():
        if len(group) < 2:
            continue
        first = _discriminators(group[0], tk)
        for other in group[1:]:
            if _discriminators(other, tk) != first:
                return None  # distinguishable -> a keyed multi-series list
    return tk


def _sort_key(value: Any) -> Tuple[int, float, str]:
    """Order numbers before strings; both ascending. Never raises."""
    if isinstance(value, (int, float)):
        return (0, float(value), '')
    return (1, 0.0, str(value))


def dedupe(items: List[dict], time_key: str) -> List[dict]:
    """
    Collapse to one entry per time key, keeping the LAST occurrence, sorted
    ascending. Last-wins because upstreams publish corrections after the
    initial value, and the corrected figure is normally appended later.
    """
    seen: Dict[Any, dict] = {}
    for entry in items:
        seen[entry.get(time_key)] = entry
    return sorted(seen.values(), key=lambda e: _sort_key(e.get(time_key)))


def normalize_payload(node: Any, _depth: int = 0) -> Any:
    """
    Recursively normalize every observation list inside `node`.

    Returns the same structure with duplicate-free, sorted series. Mutates
    lists/dicts in place where possible; the return value is authoritative.
    Depth-limited so a pathological payload can't blow the stack.
    """
    if _depth > 12:
        return node
    if isinstance(node, dict):
        for k, v in node.items():
            node[k] = normalize_payload(v, _depth + 1)
        return node
    if isinstance(node, list):
        tk = _looks_like_observations(node)
        if tk is not None:
            return dedupe(node, tk)
        return [normalize_payload(x, _depth + 1) for x in node]
    return node


def count_duplicates(node: Any, _depth: int = 0) -> int:
    """
    How many entries normalize_payload() would drop. Used by the orchestrator
    to log when an upstream starts double-publishing, so the fix can be made
    at the source instead of silently relying on the backstop.
    """
    if _depth > 12:
        return 0
    total = 0
    if isinstance(node, dict):
        for v in node.values():
            total += count_duplicates(v, _depth + 1)
    elif isinstance(node, list):
        tk = _looks_like_observations(node)
        if tk is not None:
            distinct = len({e.get(tk) for e in node})
            return len(node) - distinct
        for x in node:
            total += count_duplicates(x, _depth + 1)
    return total


def merge_by_resolution(groups: List[Tuple[int, List[dict]]],
                        time_key: str = 'ts') -> List[dict]:
    """
    Merge several same-metric series published at different resolutions.

    `groups` is [(resolution_minutes, points), ...]. Finer resolutions win on
    a shared timestamp; coarser ones still contribute timestamps the finer
    series does not cover (older days, before a resolution switch).

    This is what ENTSO-E needs: since the 15-minute MTU go-live the platform
    serves both PT60M and PT15M documents for the same window.
    """
    merged: Dict[Any, dict] = {}
    # Coarsest first so finer resolutions overwrite on collision.
    for _res, points in sorted(groups, key=lambda g: -g[0]):
        for p in points:
            merged[p.get(time_key)] = p
    return sorted(merged.values(), key=lambda e: _sort_key(e.get(time_key)))
