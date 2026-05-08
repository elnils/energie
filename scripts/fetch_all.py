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
"""
import argparse
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
    entsog, entsoe,
)


# (label, name, fetcher_callable, interval_minutes)
# interval = 0 means "always run" (use sparingly)
SCHEDULE = [
    ('SMARD',           'smard',          smard.fetch,           60),
    ('Energy-Charts',   'energy_charts',  energy_charts.fetch,   60),
    ('AGSI/ALSI Gas',   'gas_storage',    gas_storage.fetch,     360),     # 6h
    ('ENTSOG Flows',    'entsog',         entsog.fetch,          360),     # 6h
    ('Weather',         'weather',        weather.fetch,         180),     # 3h
    ('Commodities',     'commodities',    commodities.fetch,     180),     # 3h
    ('ECB FX',          'fx',             fx_ecb.fetch,          720),     # 12h
    ('Tankerkoenig',    'fuel',           fuel.fetch,            720),     # 12h
    ('Tecson Heating',  'heating_oil',    heating_oil.fetch,     720),     # 12h
    ('Destatis VPI',    'destatis_vpi',   destatis_vpi.fetch,    1440),    # 24h
    ('ENTSO-E',         'entsoe',         entsoe.fetch,          360),     # 6h (dormant w/o key)
    ('News RSS',        'news',           news.fetch,            120),     # 2h
]


def run_source(label: str, name: str, fetch_fn: Callable, force: bool = False) -> dict:
    print(f'\n[{label}]')
    state.mark_attempt(name)
    result = store.write_with_fallback(name, fetch_fn)
    if result.get('stale'):
        state.mark_error(name, result.get('last_error') or 'unknown')
    else:
        state.mark_success(name)
    return result


def write_meta(results: Dict[str, dict]) -> None:
    fresh = sum(1 for r in results.values() if not r.get('stale'))
    stale = sum(1 for r in results.values() if r.get('stale'))
    skipped = sum(1 for r in results.values() if r.get('_skipped'))
    payload = {
        'last_fetch': store.now_iso(),
        'version': '5.0',
        'fresh': fresh,
        'stale': stale,
        'skipped': skipped,
        'sources': {
            name: {
                'stale': r.get('stale', False),
                'last_success': r.get('last_success'),
                'last_error': r.get('last_error'),
                'skipped': r.get('_skipped', False),
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

    print(f'=== Energy Dashboard fetch v5.0 — {store.now_iso()} ===')
    if args.force:
        print('  (force mode: gating disabled)')
    if args.only:
        print(f'  (filter: only={args.only})')

    results: Dict[str, dict] = {}
    t_start = time.time()

    for label, name, fn, interval in SCHEDULE:
        if args.only and name != args.only:
            continue
        if not state.should_run(name, interval, force=args.force):
            last = state.get_last_success_epoch(name)
            age = (int(time.time()) - last) / 60.0 if last else None
            print(f'\n[{label}] skipped (interval={interval}m, age={age:.0f}m)')
            results[name] = {'_skipped': True, 'stale': False}
            continue
        try:
            results[name] = run_source(label, name, fn, force=args.force)
        except Exception as e:
            print(f'  !! orchestrator failed on {name}: {e}')
            # write_with_fallback handles its own exceptions; if we land here,
            # it's an unexpected programming error
            results[name] = {'stale': True, 'last_error': str(e)}

    write_meta(results)

    elapsed = time.time() - t_start
    print(f'\n=== Done in {elapsed:.1f}s — fresh={sum(1 for r in results.values() if not r.get("stale") and not r.get("_skipped"))} '
          f'stale={sum(1 for r in results.values() if r.get("stale"))} '
          f'skipped={sum(1 for r in results.values() if r.get("_skipped"))} ===')
    return 0


if __name__ == '__main__':
    sys.exit(main())
