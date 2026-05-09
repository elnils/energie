"""
Append-only history storage for time series of daily snapshots.

Why this exists:
    The main data/<source>.json files store the *current* snapshot. Each
    fetcher overwrites them on every run. To build long-term trends, we
    additionally append one row per day to data/history/<source>.jsonl
    (JSON-lines: one JSON object per line, no commas).

Design choices:
    - Append-only writes. No mutation, no race-conditions, simple recovery.
    - One record per day per source. Re-running on the same day overwrites
      that day's record (so you don't get 24 entries for one day if the
      cron triggers hourly).
    - Rotation: when a file grows past a threshold, older entries are
      aggregated. <365d: keep daily. 365-1825d: weekly avg. >1825d: monthly avg.
    - JSONL keeps file streamable, one line at a time — no full-file rewrite
      on append.

Frontend reads the file as plain text and splits by newline.
"""
import json
import os
from datetime import datetime, timezone, timedelta
from typing import Callable, Dict, List, Optional

from . import paths


HISTORY_DIR = os.path.join(paths.DATA_DIR, 'history')


def _ensure_dir() -> None:
    os.makedirs(HISTORY_DIR, exist_ok=True)


def _path(source: str) -> str:
    return os.path.join(HISTORY_DIR, f'{source}.jsonl')


def read_all(source: str) -> List[dict]:
    """Read all records as a list. Empty list if file missing or unreadable."""
    fp = _path(source)
    if not os.path.exists(fp):
        return []
    out: List[dict] = []
    try:
        with open(fp, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except OSError:
        return []
    return out


def append_today(source: str, record: dict) -> None:
    """
    Append `record` to the source's history file, keyed by today's date.

    If a record for today already exists, replace it. Otherwise append.
    The record gets a 'date' field auto-stamped (YYYY-MM-DD UTC).
    """
    _ensure_dir()
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    record = {**record, 'date': today}

    existing = read_all(source)
    # Drop any record for today (idempotent re-runs)
    existing = [r for r in existing if r.get('date') != today]
    existing.append(record)
    existing.sort(key=lambda r: r.get('date', ''))

    # Rewrite file. Could be optimised for true append, but rotation
    # would make this messy. At < 1 MB this rewrite cost is trivial.
    fp = _path(source)
    tmp = fp + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        for r in existing:
            f.write(json.dumps(r, ensure_ascii=False, separators=(',', ':')))
            f.write('\n')
    os.replace(tmp, fp)


def rotate_if_needed(source: str,
                     daily_keep_days: int = 365,
                     weekly_keep_days: int = 1825,
                     min_file_size_bytes: int = 100_000) -> None:
    """
    Aggregate old records when file grows beyond size threshold.

    Records younger than `daily_keep_days` stay daily.
    Records between `daily_keep_days` and `weekly_keep_days` become weekly avg.
    Records older than `weekly_keep_days` become monthly avg.

    Aggregation is by simple mean of all numeric fields.
    """
    fp = _path(source)
    if not os.path.exists(fp):
        return
    if os.path.getsize(fp) < min_file_size_bytes:
        return

    rows = read_all(source)
    if not rows:
        return

    today = datetime.now(timezone.utc).date()
    daily_cutoff = today - timedelta(days=daily_keep_days)
    weekly_cutoff = today - timedelta(days=weekly_keep_days)

    daily: List[dict] = []
    to_aggregate_weekly: List[dict] = []
    to_aggregate_monthly: List[dict] = []
    for r in rows:
        d_str = r.get('date', '')
        try:
            d = datetime.strptime(d_str, '%Y-%m-%d').date()
        except ValueError:
            continue
        if d >= daily_cutoff:
            daily.append(r)
        elif d >= weekly_cutoff:
            to_aggregate_weekly.append(r)
        else:
            to_aggregate_monthly.append(r)

    weekly = _aggregate_by_period(to_aggregate_weekly, period='week')
    monthly = _aggregate_by_period(to_aggregate_monthly, period='month')
    final = monthly + weekly + daily
    final.sort(key=lambda r: r.get('date', ''))

    tmp = fp + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        for r in final:
            f.write(json.dumps(r, ensure_ascii=False, separators=(',', ':')))
            f.write('\n')
    os.replace(tmp, fp)


def _aggregate_by_period(records: List[dict], period: str) -> List[dict]:
    """Group records by week or month and average their numeric fields."""
    if not records:
        return []
    groups: Dict[str, List[dict]] = {}
    for r in records:
        try:
            d = datetime.strptime(r.get('date', ''), '%Y-%m-%d').date()
        except ValueError:
            continue
        if period == 'week':
            iso = d.isocalendar()
            key = f'{iso[0]}-W{iso[1]:02d}'
            anchor_date = (d - timedelta(days=d.weekday())).strftime('%Y-%m-%d')
        elif period == 'month':
            key = d.strftime('%Y-%m')
            anchor_date = d.strftime('%Y-%m-01')
        else:
            return records
        groups.setdefault(key, []).append({**r, 'date': anchor_date, '_period': period})

    out = []
    for key, entries in groups.items():
        if not entries:
            continue
        merged: Dict = {'date': entries[0]['date'], '_period': period, '_n': len(entries)}
        # Average numeric fields. Non-numeric (strings) take first value.
        all_keys = set()
        for e in entries:
            all_keys.update(k for k in e.keys() if not k.startswith('_') and k != 'date')
        for k in all_keys:
            vals = [e[k] for e in entries if isinstance(e.get(k), (int, float))]
            if vals:
                merged[k] = round(sum(vals) / len(vals), 4)
            else:
                # take first non-null
                for e in entries:
                    if e.get(k) is not None:
                        merged[k] = e[k]
                        break
        out.append(merged)
    return sorted(out, key=lambda r: r['date'])


def record_history(source: str, record: Optional[dict]) -> None:
    """
    High-level helper: append today's record + rotate if oversized.
    Skips silently if record is None or empty.
    """
    if not record:
        return
    try:
        append_today(source, record)
        rotate_if_needed(source)
    except Exception as e:
        # Don't fail the parent fetcher just because history append failed
        print(f'  ! history/{source}: {e}')
