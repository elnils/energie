"""
GIE AGSI+ — multi-year daily gas storage history for the seasonal comparison.

gas_storage.py keeps the latest ~300 days per country (one API page). The
seasonal chart ("net injection 2026 against 2025 and the 2017–2023 range")
needs every year back to 2017, so this module pages through the archive
once and then only tops up the recent days on each run.

Storage format (compact on purpose — ten years of daily rows per country):

    countries: { de: { name, rows: [[date, fill_pct, injection, withdrawal,
                                     gas_in_storage_twh], ...],
                       backfilled_to: '2017-01-01' } }

injection / withdrawal are GWh/d as AGSI reports them; net injection is
derived in the browser (injection - withdrawal) so no redundant column is
committed.

Backfill is resumable: each run spends at most MAX_REQUESTS calls, walking
backwards year by year from the oldest stored date, so a slow API or a
budget kill never loses what was already fetched — the next run continues
where this one stopped. Existing rows are never deleted; new values for a
date overwrite the old ones (AGSI revises recent days).
"""
import os
import time
from datetime import date, timedelta
from typing import Dict, List, Optional

from core import http, store
from fetchers.gas_storage import _normalize_entry

API = 'https://agsi.gie.eu/api'
START = date(2017, 1, 1)
MAX_REQUESTS = 90          # per run; well inside the 240 s budget
TOPUP_DAYS = 21            # re-read this many recent days (AGSI revisions)

COUNTRIES = {
    'eu': 'EU gesamt',
    'de': 'Deutschland',
    'at': 'Österreich',
    'nl': 'Niederlande',
    'fr': 'Frankreich',
    'it': 'Italien',
}


class _Budget:
    def __init__(self, n: int):
        self.left = n

    def take(self) -> bool:
        if self.left <= 0:
            return False
        self.left -= 1
        return True


def _page(api_key: str, params: dict) -> dict:
    s = http.get_session()
    r = s.get(API, headers={'x-key': api_key}, params=params, timeout=30)
    if not r.ok:
        raise RuntimeError(f'HTTP {r.status_code}: {r.text[:160]}')
    p = r.json()
    return p if isinstance(p, dict) else {'data': p}


def _range(api_key: str, code: str, frm: date, to: date, budget: _Budget) -> Optional[List[dict]]:
    """All rows between frm and to, following pagination. None = budget out."""
    out: List[dict] = []
    page = 1
    while True:
        if not budget.take():
            return None
        params = {'from': frm.isoformat(), 'to': to.isoformat(), 'size': 300, 'page': page}
        if code == 'eu':
            params['type'] = 'eu'
        else:
            params['country'] = code.upper()
        p = _page(api_key, params)
        raw = p.get('data') or []
        for e in raw:
            n = _normalize_entry(e)
            if n:
                out.append(n)
        last_page = int(p.get('last_page') or 1)
        if page >= last_page or not raw:
            break
        page += 1
        time.sleep(0.15)
    return out


def _to_row(n: dict) -> list:
    return [n['date'], n.get('fill_pct'), n.get('injection'), n.get('withdrawal'),
            n.get('gas_in_storage_twh')]


def fetch() -> dict:
    api_key = os.environ.get('GIE_API_KEY', '').strip()
    if len(api_key) < 10:
        raise RuntimeError('GIE_API_KEY missing or too short — register at agsi.gie.eu')

    prev = (store.read_json('gas_storage_history') or {}).get('data', {}).get('countries', {})
    budget = _Budget(MAX_REQUESTS)
    today = date.today()
    out: Dict[str, dict] = {}
    errors: Dict[str, str] = {}

    for code, name in COUNTRIES.items():
        node = prev.get(code) or {}
        rows = {r[0]: r for r in node.get('rows', [])}
        backfilled_to = node.get('backfilled_to')
        try:
            # 1. top up the recent days
            last = max(rows) if rows else None
            frm = (date.fromisoformat(last) - timedelta(days=TOPUP_DAYS)) if last \
                else today - timedelta(days=365)
            got = _range(api_key, code, frm, today, budget)
            if got:
                for n in got:
                    rows[n['date']] = _to_row(n)
            # 2. walk backwards one year per step until START
            oldest = date.fromisoformat(min(rows)) if rows else today
            while oldest > START and budget.left > 0:
                seg_from = max(START, date(oldest.year - 1, oldest.month, 1))
                got = _range(api_key, code, seg_from, oldest - timedelta(days=1), budget)
                if got is None:
                    break
                for n in got:
                    rows[n['date']] = _to_row(n)
                if not got:
                    # Nothing earlier exists for this country — stop asking.
                    backfilled_to = seg_from.isoformat()
                    break
                oldest = seg_from
                backfilled_to = seg_from.isoformat()
        except Exception as e:
            errors[code] = f'{type(e).__name__}: {e}'[:200]
            print(f'  ! agsi-history/{code}: {errors[code]}')

        ordered = [rows[d] for d in sorted(rows)]
        out[code] = {'name': name, 'rows': ordered, 'backfilled_to': backfilled_to}
        span = f'{ordered[0][0]}..{ordered[-1][0]}' if ordered else '—'
        print(f'    agsi-history/{code}: {len(ordered)} days {span} '
              f'(requests left {budget.left})')

    if not any(n['rows'] for n in out.values()):
        raise RuntimeError(f'no AGSI history rows: {errors}')

    complete = all((n.get('backfilled_to') or '9999') <= START.isoformat()
                   for n in out.values() if n['rows'])
    return {
        'data': {'countries': out},
        'meta': {
            'source': 'GIE AGSI+',
            'columns': ['date', 'fill_pct', 'injection_gwh_d', 'withdrawal_gwh_d',
                        'gas_in_storage_twh'],
            'start': START.isoformat(),
            'backfill_complete': complete,
            'errors': errors,
        },
    }
