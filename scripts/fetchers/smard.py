"""
SMARD — German federal grid agency electricity statistics.

Public JSON API (no key). 15-minute resolution per indicator. We fetch the
last 7 days for each filter and assemble per-source generation series.

Filters added vs original:
  - pumped_storage (4070): Pumpspeicher — negative = charging, positive = discharge
  - other_conventional (1228): Sonstige Konventionelle
  - other_renewables (4073): Sonstige Erneuerbare
  Total: 13 generation/load filters (was 10)
"""
import time
from datetime import datetime, timedelta
from typing import Dict, List

import pytz

from core import history, http, validators


FILTERS: Dict[str, int] = {
    # Renewables
    'wind_onshore':      4067,
    'wind_offshore':     1225,
    'solar':             4068,
    'biomass':           4066,
    'hydro':             1226,
    'other_renewables':  4073,   # ← new
    # Conventional
    'nuclear':           1224,
    'lignite':           1223,
    'hard_coal':         4069,
    'natural_gas':       4071,
    'pumped_storage':    4070,   # ← new (negative = charging)
    'other_conventional':1228,  # ← new
    # Demand
    'load':              410,
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
            # pumped_storage can be negative (charging mode), so use gen_mw_signed
            if not validators.in_range('gen_mw', abs(fv)):
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

    _record_history(out)

    return {
        'data': {
            'series': out,
        },
        'meta': {
            'source': 'Bundesnetzagentur SMARD',
            'license': 'CC BY 4.0',
            'url': 'https://www.smard.de',
            'units': 'MW (15-min resolution)',
            'filters': {k: v for k, v in FILTERS.items()},
            'note': (
                'pumped_storage: negative = charging (consuming grid power), '
                'positive = discharging (feeding into grid). '
                'other_renewables: geothermal, tidal, and minor sources.'
            ),
        },
    }


# Emission factors in g CO2 per kWh, lifecycle basis. Same numbers the
# dashboard uses for its CO2-intensity chart, kept here so the archived
# figure and the drawn one cannot drift apart.
CO2_G_PER_KWH = {
    'lignite': 900, 'hard_coal': 800, 'natural_gas': 400, 'nuclear': 5,
    'wind_onshore': 18, 'wind_offshore': 18, 'solar': 18, 'biomass': 18,
    'hydro': 18, 'other_renewables': 18,
}
RENEWABLE_KEYS = ('wind_onshore', 'wind_offshore', 'solar', 'biomass',
                  'hydro', 'other_renewables')


def _record_history(series: dict) -> None:
    """
    Archive today's generation mix to data/history/smard_mix.jsonl.

    SMARD only serves a rolling window of roughly two weeks at 15-minute
    resolution, so the mix for any given past day is unrecoverable once it
    rolls off. The daily aggregate — energy per source, renewable share and
    the resulting CO2 intensity — is what a long-run view actually needs,
    and it is three orders of magnitude smaller than the raw series.
    """
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    def todays_points(key):
        return [p for p in (series.get(key) or [])
                if p.get('v') is not None
                and datetime.fromtimestamp(p['ts'] / 1000, timezone.utc)
                            .strftime('%Y-%m-%d') == today]

    record: dict = {}
    energy_mwh: dict = {}
    for key in list(CO2_G_PER_KWH) + ['pumped_storage', 'other_conventional', 'load']:
        pts = todays_points(key)
        if not pts:
            continue
        # 15-minute averages in MW -> MWh for the day so far.
        mwh = sum(p['v'] for p in pts) * 0.25
        energy_mwh[key] = round(mwh, 1)
        record[f'{key}_mwh'] = round(mwh, 1)
    if not energy_mwh:
        return

    generation = {k: v for k, v in energy_mwh.items() if k in CO2_G_PER_KWH}
    total = sum(generation.values())
    if total > 0:
        ren = sum(v for k, v in generation.items() if k in RENEWABLE_KEYS)
        record['renewable_share_pct'] = round(ren / total * 100, 2)
        record['co2_g_per_kwh'] = round(
            sum(v * CO2_G_PER_KWH[k] for k, v in generation.items()) / total, 1)
        record['generation_mwh'] = round(total, 1)
    record['hours_covered'] = round(len(todays_points('load')) * 0.25, 2)
    history.record_history('smard_mix', record)
