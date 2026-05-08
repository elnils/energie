"""
State-based frequency gating.

`data/_state.json` records last_attempt and last_success per source. The
orchestrator checks `should_run(name, interval_minutes)` and skips sources
that don't need refreshing yet. This lets us run cron hourly while updating
slow sources only every 6h or 24h.

`force=True` (e.g., manual workflow_dispatch) bypasses gating.
"""
import json
import os
import time
from datetime import datetime
from typing import Optional

from . import paths


def _read() -> dict:
    if not os.path.exists(paths.STATE_FILE):
        return {}
    try:
        with open(paths.STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _write(state: dict) -> None:
    os.makedirs(paths.DATA_DIR, exist_ok=True)
    with open(paths.STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)


def _epoch() -> int:
    return int(time.time())


def should_run(name: str, interval_minutes: int, force: bool = False) -> bool:
    """Return True if `name` is due for a refresh."""
    if force:
        return True
    state = _read()
    last = state.get(name, {}).get('last_success_epoch')
    if not last:
        return True
    return _epoch() - last >= interval_minutes * 60


def mark_attempt(name: str) -> None:
    state = _read()
    entry = state.setdefault(name, {})
    entry['last_attempt_epoch'] = _epoch()
    entry['last_attempt_iso'] = datetime.utcnow().isoformat(timespec='seconds') + 'Z'
    _write(state)


def mark_success(name: str) -> None:
    state = _read()
    entry = state.setdefault(name, {})
    entry['last_success_epoch'] = _epoch()
    entry['last_success_iso'] = datetime.utcnow().isoformat(timespec='seconds') + 'Z'
    _write(state)


def mark_error(name: str, message: str) -> None:
    state = _read()
    entry = state.setdefault(name, {})
    entry['last_error_epoch'] = _epoch()
    entry['last_error_message'] = message[:500]
    _write(state)


def snapshot() -> dict:
    return _read()


def get_last_success_epoch(name: str) -> Optional[int]:
    return _read().get(name, {}).get('last_success_epoch')
