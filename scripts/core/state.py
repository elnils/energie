"""
Atomic writes and stale-while-error wrapper.

The contract every fetcher follows:

    result = store.write_with_fallback(name, fetch_fn)

If `fetch_fn()` returns a dict, we validate it (caller-side) and write it
atomically to data/<name>.json plus a tmp staging step. If the function raises,
we read the previous data/<name>.json and rewrite it with `stale: true` and
`last_error` populated — the dashboard keeps showing the last good values
instead of breaking.

Defence-in-depth: we also enforce a per-source schema sanity check. If a
fetcher accidentally returns data shaped like another source (e.g. ENTSOG-like
'points' field appearing in gas_storage output), we refuse to write it.
"""
import json
import os
import tempfile
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Set

from . import paths


# Minimum-required-keys per data source. Each set lists keys that MUST be
# present at the top of the `data` dict. If any are missing OR if foreign
# keys (from another source) appear, validation rejects.
EXPECTED_KEYS: Dict[str, Set[str]] = {
    'gas_storage':   {'gas'},                       # plus optional 'lng'
    'entsog':        {'points'},                    # plus optional 'errors'
    'entsoe':        {'flows_in', 'flows_out'},     # OR {'awaiting_key'}
    'fuel':          {'cities'},
    'fx':            {'current'},
    'heating_oil':   {'reference_price_eur_l'},
    'destatis_vpi':  {'energy_series'},
    'smard':         {'series'},
    'energy_charts': {'price_de'},
    'commodities':   {'brent_crude'},
    'weather':       {'cities'},
    'news':          {'articles'},
    # New 2026: jet-fuel monitoring stack
    'eia_petroleum': {'jet_fuel_us_gulf_weekly'},
    'fred_energy':   {'DJFUELUSGULF'},
    'eurostat_oil':  {'jet_stocks'},
}

FORBIDDEN_KEYS: Dict[str, Set[str]] = {
    'gas_storage':   {'points', 'flows_in', 'articles', 'cities'},
    'entsog':        {'gas', 'flows_in', 'articles'},
    'entsoe':        {'points', 'gas', 'articles'},
    'fuel':          {'gas', 'points', 'articles'},
    'fx':            {'gas', 'points', 'articles', 'cities'},
    'heating_oil':   {'gas', 'points', 'articles'},
    'destatis_vpi':  {'gas', 'points', 'articles'},
    'smard':         {'gas', 'points', 'articles'},
    'energy_charts': {'gas', 'points', 'articles'},
    'commodities':   {'gas', 'points', 'articles'},
    'weather':       {'gas', 'points', 'articles'},
    'news':          {'gas', 'points', 'cities'},
    'eia_petroleum': {'gas', 'points', 'articles', 'cities'},
    'fred_energy':   {'gas', 'points', 'articles', 'cities'},
    'eurostat_oil':  {'gas', 'points', 'articles', 'cities'},
}


def _schema_check(name: str, data: Any) -> None:
    """Raise ValueError if `data` doesn't look like it belongs in <name>.json."""
    if not isinstance(data, dict):
        return  # legacy / non-dict payloads pass through
    expected = EXPECTED_KEYS.get(name, set())
    forbidden = FORBIDDEN_KEYS.get(name, set())

    keys = set(data.keys())
    bad = keys & forbidden
    if bad:
        raise ValueError(
            f'fetcher {name!r} returned foreign keys {bad!r} — '
            f'this looks like another source\'s data. Refusing to write.'
        )
    # Allow the entsoe special "awaiting_key" empty-state shape
    if name == 'entsoe' and 'awaiting_key' in data:
        return
    if expected and not (expected & keys):
        raise ValueError(
            f'fetcher {name!r} missing expected keys {expected!r}; got {keys!r}'
        )


def _ensure_dirs() -> None:
    os.makedirs(paths.DATA_DIR, exist_ok=True)
    os.makedirs(paths.TMP_DIR, exist_ok=True)


def read_json(name: str) -> Optional[dict]:
    """Return the previous payload for `name` or None if not present/parseable."""
    fp = os.path.join(paths.DATA_DIR, f'{name}.json')
    if not os.path.exists(fp):
        return None
    try:
        with open(fp, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as exc:
        print(f'  ! could not read previous {name}.json: {exc}')
        return None


def write_atomic(name: str, payload: dict) -> str:
    """Write payload to data/<name>.json atomically. Returns the final path."""
    _ensure_dirs()
    final_path = os.path.join(paths.DATA_DIR, f'{name}.json')
    fd, tmp_path = tempfile.mkstemp(prefix=f'{name}_', suffix='.json', dir=paths.TMP_DIR)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))
        os.replace(tmp_path, final_path)
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise
    return final_path


def now_iso() -> str:
    return datetime.now(paths.UTC).isoformat(timespec='seconds')


def write_with_fallback(
    name: str,
    fetch_fn: Callable[[], dict],
) -> dict:
    """
    Run fetch_fn. On success: validate schema, write its result.
    On failure: read previous file, mark `stale: true`, preserve `data` field,
    write that. Always writes — never leaves dashboard with no file.
    """
    prev = read_json(name) or {}
    try:
        fresh = fetch_fn()
        if not isinstance(fresh, dict) or 'data' not in fresh:
            raise ValueError(f'fetcher {name} did not return dict with "data" key')
        # Hard schema check — refuse to write data shaped like another source
        _schema_check(name, fresh.get('data'))
        out = {
            'updated': fresh.get('updated') or now_iso(),
            'stale': False,
            'last_success': now_iso(),
            'last_error': None,
            'data': fresh['data'],
        }
        if 'meta' in fresh:
            out['meta'] = fresh['meta']
        write_atomic(name, out)
        print(f'  v {name}.json written ({_size_kb(name)} KB)')
        return out
    except Exception as exc:
        # Stale-while-error: keep previous data, mark as stale
        out = {
            'updated': now_iso(),
            'stale': True,
            'last_success': prev.get('last_success'),
            'last_error': f'{type(exc).__name__}: {exc}',
            'data': prev.get('data', {}),
        }
        if 'meta' in prev:
            out['meta'] = prev['meta']
        write_atomic(name, out)
        print(f'  ! {name} failed ({exc}) — kept previous, stale=true')
        return out


def _size_kb(name: str) -> int:
    fp = os.path.join(paths.DATA_DIR, f'{name}.json')
    return os.path.getsize(fp) // 1024 if os.path.exists(fp) else 0
