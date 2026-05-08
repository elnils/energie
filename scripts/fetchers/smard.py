"""
SMARD — German federal grid agency electricity statistics.

Public JSON API (no key). 15-minute resolution per indicator. We fetch the
last 7 days for each filter and assemble per-source generation series.
"""
import time
from datetime import datetime, timedelta
from typing import Dict, List

import pytz

from core import http, validators


FILTERS: Dict[str, int] = {
    'wind_onshore': 4067, 'wind_offshore': 1225, 'solar': 4068,
    'biomass': 4066, 'hydro': 1226, 'nuclear': 1224,
    'lignite': 1223, 'hard_coal': 4069, 'natural_gas': 4071,
    'load': 410,
}


def _bucket_starts(now_berlin: datetime) -> List[int]:
    """SMARD splits data in weekly buckets aligned to Mondays. Return the
    last two Mondays as ms-since-epoch."""
    monday_this_week = (now_berlin - timedelta(days=now_berlin.weekday())) \
        .replace(hour=0, minute=0, second=0, microsecond=0)
    monday_last_week = monday_this_week - timedelta(days=7)
    return [
        int(monday_last_week.timestamp() * 1000),
        int(monday_this_week.timestamp() * 1000),
    ]


def _fetch_filter(filter_id: int, buckets: List[int]) -> List[dict]:
    s = http.get_session()
    series: List[dict] = []
    seen = set()
    # Get index to know which buckets are available
    idx_url = f'https://www.smard.de/app/chart_data/{filter_id}/DE/index_quarterhour.json'
    idx = s.get(idx_url, timeout=20).json().get('timestamps', [])
    available = [b for b in buckets if b in idx]
    if not available:
        # Fall back to most recent two
        available = idx[-2:] if len(idx) >= 2 else idx
    for bucket in available:
        url = (f'https://www.smard.de/app/chart_data/{filter_id}/DE/'
               f'{filter_id}_DE_quarterhour_{bucket}.json')
        try:
            data = s.get(url, timeout=20).json().get('series', [])
        except Exception as e:
            print(f'      bucket {bucket}: {e}')
            continue
        for entry in data:
            if not (isinstance(entry, list) and len(entry) == 2):
                continue
            ts, val = entry
            if val is None or ts in seen:
                continue
            seen.add(ts)
            try:
                fv = round(float(val), 2)
            except (TypeError, ValueError):
                continue
            # Range check: MW for generation/load; cap at 150 GW = 150_000 MW
            if not validators.in_range('gen_mw', fv):
                continue
            series.append({'ts': ts, 'v': fv})
        time.sleep(0.05)
    series.sort(key=lambda x: x['ts'])
    return series


def fetch() -> dict:
    berlin = pytz.timezone('Europe/Berlin')
    now_b = datetime.now(berlin)
    buckets = _bucket_starts(now_b)
    out: Dict[str, List[dict]] = {}
    for name, fid in FILTERS.items():
        try:
            out[name] = _fetch_filter(fid, buckets)
            print(f'    smard/{name}: {len(out[name])} pts')
        except Exception as e:
            print(f'  ! smard/{name}: {e}')
            out[name] = []

    # Ensure at least one source has data, otherwise raise
    if not any(out.values()):
        raise RuntimeError('SMARD: all filters returned empty')

    return {
        'data': {
            'series': out,
        },
        'meta': {
            'source': 'Bundesnetzagentur SMARD',
            'license': 'CC BY 4.0',
            'units': 'MW (15-min resolution)',
        },
    }
