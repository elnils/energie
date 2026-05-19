"""
Energy Dashboard fetcher — orchestrator.

Architecture:
  - Each source has its own module under fetchers/.
  - This script reads the SCHEDULE table and asks state.should_run() per source.
  - For each due source, runs core.store.write_with_fallback() which guarantees
    that data/<source>.json always exists with a usable schema (stale or fresh).
  - Updates _state.json after each successful run.
  - Writes meta.json with overall status.

CLI:
  python scripts/fetch_all.py             # honours frequency gating
  python scripts/fetch_all.py --force     # ignores gating, runs everything
  python scripts/fetch_all.py --only=ecb  # runs only one source

Cron strategy: run hourly. Sources that need 6h/24h refresh skip themselves.

v5.2 — per-source wallclock budgets:
  A hanging upstream API can no longer block the whole run. Each source is
  guarded by a SIGALRM-based watchdog. On timeout, the source is marked
  stale with a TimeoutError and the loop continues. POSIX-only — fine for
  the GitHub Actions ubuntu-latest runner; would need rework for Windows.
"""
import argparse
import signal
import sys
import os
import time
from datetime import datetime
from typing import Callable, Dict

# Local imports
sys.path.insert(0, os.path.dirname(__file__))
from core import store, state, paths
from fetchers import (
    smard, energy_charts, gas_storage, fuel, weather,
    commodities, news, fx_ecb, heating_oil, destatis_vpi,
    entsog, entsoe, eia_petroleum, fred_energy, eurostat_oil,
    energy_futures,
)


# (label, name, fetcher_callable, interval_minutes, budget_seconds)
#
# budget_seconds tuned per source profile:
#   - 60s   small/fast APIs (single endpoint, few calls)
#   - 90s   medium (multiple endpoints, light parsing)
#   - 180s  Tankerkoenig (18 cities × 3 fuel types × retries-on-503)
#   - 120s  weather (8 cities × open-meteo, usually fast but tolerate hiccups)
#
# Sum of budgets ~ 22min worst case; matches the workflow's 15min job timeout
# expectation that any single bad source can at most burn ITS budget, never
# the whole run.
SCHEDULE = [
    ('SMARD',            'smard',           smard.fetch,            60,    90),
    ('Energy-Charts',    'energy_charts',   energy_charts.fetch,    60,    90),
    ('AGSI/ALSI Gas',    'gas_storage',     gas_storage.fetch,      360,   90),
    ('ENTSOG Flows',     'entsog',          entsog.fetch,           360,   120),
    ('Weather',          'weather',         weather.fetch,          180,   120),
    ('Commodities',      'commodities',     commodities.fetch,      180,   90),
    ('ECB FX',           'fx',              fx_ecb.fetch,           720,   60),
    ('Tankerkoenig',     'fuel',            fuel.fetch,             720,   180),
    ('Tecson Heating',   'heating_oil',     heating_oil.fetch,      720,   60),
    ('Destatis VPI',     'destatis_vpi',    destatis_vpi.fetch,     1440,  60),
    ('ENTSO-E',          'entsoe',          entsoe.fetch,           360,   120),
    ('FRED Energy',      'fred_energy',     fred_energy.fetch,      720,   60),
    ('EIA Petroleum',    'eia_petroleum',   eia_petroleum.fetch,    720,   90),
    ('Energy Futures',   'energy_futures',  energy_futures.fetch,   360,   90),
    ('Eurostat Oil',     'eurostat_oil',    eurostat_oil.fetch,     1440,  90),
    ('News RSS',         'news',            news.fetch,             120,   90),
]


class SourceTimeoutError(Exception):
    """Raised by the SIGALRM handler when a source exceeds its budget."""
    pass


def _alarm_handler(signum, frame):
    # The handler runs in the main thread when SIGALRM fires. Raising here
    # unwinds whatever the fetcher was doing — including time.sleep() and
    # blocked socket reads in requests (which releases the GIL during I/O).
    raise SourceTimeoutError('source exceeded wallclock budget')


def run_source(label: str, name: str, fetch_fn: Callable, budget_s: int,
               force: bool = False) -> dict:
    """
    Run one source with a hard wallclock budget. The budget enforces an upper
    bound on how long a single hung API can stall the orchestrator. On budget
    expiry, write_with_fallback's stale-on-error path kicks in (because the
    TimeoutError propagates out of fetch_fn into store.write_with_fallback's
    try/except), so the dashboard keeps showing previous data.
    """
    print(f'\n[{label}] (budget {budget_s}s)')
    state.mark_attempt(name)

    # Install per-source alarm. Reset to 0 in the finally block — leaking
    # an alarm into the next source's window would be a nightmare to debug.
    signal.signal(signal.SIGALRM, _alarm_handler)
    signal.alarm(budget_s)
    t0 = time.time()
    try:
        result = store.write_with_fallback(name, fetch_fn)
    finally:
        # Disable any pending alarm BEFORE we touch state/result handling,
        # so no residual SIGALRM can fire during mark_success/mark_error.
        signal.alarm(0)

    elapsed = time.time() - t0
    if result.get('stale'):
        err = result.get('last_error') or 'unknown'
        state.mark_error(name, err)
        # Distinguish a budget kill from a normal upstream error in the log.
        if 'SourceTimeoutError' in err or 'wallclock budget' in err:
            print(f'  !! {name}: BUDGET KILL after {elapsed:.1f}s — marked stale')
        else:
            print(f'  !! {name}: stale after {elapsed:.1f}s — {err}')
    else:
        state.mark_success(name)
        print(f'  ok {name}: fresh ({elapsed:.1f}s)')
    return result


def write_meta(results: Dict[str, dict]) -> None:
    fresh   = sum(1 for r in results.values() if not r.get('stale') and not r.get('_skipped'))
    stale   = sum(1 for r in results.values() if r.get('stale'))
    skipped = sum(1 for r in results.values() if r.get('_skipped'))
    payload = {
        'last_fetch': store.now_iso(),
        'version': '5.2',
        'fresh': fresh,
        'stale': stale,
        'skipped': skipped,
        'sources': {
            name: {
                'stale':        r.get('stale', False),
                'last_success': r.get('last_success'),
                'last_error':   r.get('last_error'),
                'skipped':      r.get('_skipped', False),
            } for name, r in results.items()
        },
    }
    store.write_atomic('meta', payload)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--force', action='store_true',
                        help='Ignore frequency gating, run all sources')
    parser.add_argument('--only', type=str, default=None,
                        help='Run only a single source (by name)')
    args = parser.parse_args()

    # POSIX guard: signal.SIGALRM is unix-only. Fail fast on Windows rather
    # than have the watchdog silently no-op.
    if not hasattr(signal, 'SIGALRM'):
        print('!! fetch_all.py requires POSIX (SIGALRM). Run on Linux/macOS.')
        return 2

    print(f'=== Energy Dashboard fetch v5.2 — {store.now_iso()} ===')
    if args.force:
        print('  (force mode: gating disabled)')
    if args.only:
        print(f'  (filter: only={args.only})')

    results: Dict[str, dict] = {}
    t_start = time.time()

    for label, name, fn, interval, budget in SCHEDULE:
        if args.only and name != args.only:
            continue
        if not state.should_run(name, interval, force=args.force):
            last    = state.get_last_success_epoch(name)
            age     = (int(time.time()) - last) / 60.0 if last else None
            age_str = f'{age:.0f}m' if age is not None else '?'
            print(f'\n[{label}] skipped (interval={interval}m, age={age_str})')
            results[name] = {'_skipped': True, 'stale': False}
            continue
        try:
            results[name] = run_source(label, name, fn, budget, force=args.force)
        except Exception as e:
            # Catch-all so one source's catastrophic failure can never
            # abort the orchestrator. write_with_fallback handles most
            # errors itself; this is for the unexpected.
            print(f'  !! orchestrator failed on {name}: {type(e).__name__}: {e}')
            results[name] = {'stale': True, 'last_error': f'{type(e).__name__}: {e}'}
            # Defensive: make sure no alarm survives a catch-all path.
            try:
                signal.alarm(0)
            except Exception:
                pass

    write_meta(results)

    elapsed = time.time() - t_start
    fresh   = sum(1 for r in results.values() if not r.get('stale') and not r.get('_skipped'))
    stale   = sum(1 for r in results.values() if r.get('stale'))
    skipped = sum(1 for r in results.values() if r.get('_skipped'))
    print(f'\n=== Done in {elapsed:.1f}s — fresh={fresh} stale={stale} skipped={skipped} ===')
    return 0


if __name__ == '__main__':
    sys.exit(main())
