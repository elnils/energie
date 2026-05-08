"""
Atomic writes and stale-while-error wrapper.

The contract every fetcher follows:

    result = store.write_with_fallback(name, fetch_fn)

If `fetch_fn()` returns a dict, we validate it (caller-side) and write it
atomically to data/<name>.json plus a tmp staging step. If the function raises,
we read the previous data/<name>.json and rewrite it with `stale: true` and
`last_error` populated — the dashboard keeps showing the last good values
instead of breaking.
"""
import json
import os
import tempfile
from datetime import datetime
from typing import Any, Callable, Optional

from . import paths


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
    Run fetch_fn. On success: write its result.
    On failure: read previous file, mark `stale: true`, preserve `data` field,
    write that. Always writes — never leaves dashboard with no file.

    The fetcher is expected to return a dict with at least:
        {"updated": "<iso>", "data": {...}}

    Wrapped output adds:
        "stale": bool
        "last_success": "<iso>"
        "last_error": str | None
    """
    prev = read_json(name) or {}
    try:
        fresh = fetch_fn()
        if not isinstance(fresh, dict) or 'data' not in fresh:
            raise ValueError(f'fetcher {name} did not return dict with "data" key')
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
