"""
ENTSO-E Transparency Platform — European electricity market data.

Uses the official entsoe-py client (pip install entsoe-py pandas).
Requires ENTSOE_SECURITY_TOKEN environment variable (GitHub Secret).

To obtain a token:
  Email: transparency@entsoe.eu
  Subject: "Restful API access request"
  Wait: 1–3 business days.

Data fetched (all for Germany DE_LU unless noted):
  1. Day-ahead prices        DE/AT/FR/PL/CH         14 days
  2. Generation mix          DE  (15+ fuel types)    7 days
  3. Actual load             DE                       7 days
  4. Wind + solar forecast   DE  (day-ahead)         48h back + 48h fwd
  5. Cross-border flows      DE↔FR/AT/PL/DK/NL/CZ  48 hours
  6. Net position (export+)  DE                       7 days
  7. Installed capacity      DE  (current year snap)  latest row

Output schema  data/entsoe.json  (wrapped in v5 store.py envelope):
{
  "day_ahead_prices": {
    "DE_LU": [{ts, v}, ...],  // EUR/MWh, hourly
    "AT":    [...],
    "FR":    [...],
    "PL":    [...],
    "CH":    [...]
  },
  "generation": {             // MW per 15min/60min bucket
    "solar":            [{ts,v},...],
    "wind_onshore":     [...],
    "wind_offshore":    [...],
    "nuclear":          [...],
    "lignite":          [...],
    "hard_coal":        [...],
    "natural_gas":      [...],
    "hydro_run_of_river":[...],
    "hydro_reservoir":  [...],
    "hydro_pumped_storage":[...],
    "biomass":          [...],
    "other_renewable":  [...],
    "other":            [...],
    "oil":              [...],
    "waste":            [...]
  },
  "load":                [{ts, v}, ...],    // MW
  "wind_solar_forecast": {
    "wind_onshore":  [...],
    "wind_offshore": [...],
    "solar":         [...]
  },
  "crossborder_flows": {
    "DE_LU->FR":    [{ts,v},...],  // MW  positive = export
    "FR->DE_LU":    [...],
    ... (12 direction pairs)
  },
  "net_position":  [{ts, v}, ...],  // MW positive = net exporter
  "installed_capacity": {            // MW, latest published
    "solar": 70000, "wind_onshore": 59000, ...
  }
}
"""
import math
import os
import time
from datetime import datetime, timezone
from typing import List, Optional

import pytz


# ── Helpers ────────────────────────────────────────────────────────────────────

def _series_to_pairs(series) -> List[dict]:
    """
    pandas Series (DatetimeIndex) → [{ts: int, v: float}].
    ts = Unix epoch seconds. NaN values are dropped silently.
    """
    result = []
    try:
        import pandas as pd
        for idx, val in series.items():
            if pd.isna(val):
                continue
            try:
                result.append({'ts': int(idx.timestamp()), 'v': round(float(val), 2)})
            except Exception:
                pass
    except Exception as e:
        print(f'      _series_to_pairs error: {e}')
    return result


def _df_column_to_pairs(df, col) -> List[dict]:
    """Extract one column from a DataFrame → [{ts, v}], dropping NaN."""
    try:
        import pandas as pd
        series = df[col].dropna()
        pairs = []
        for idx, val in series.items():
            try:
                pairs.append({'ts': int(idx.timestamp()), 'v': round(float(val), 2)})
            except Exception:
                pass
        return pairs
    except Exception as e:
        print(f'      _df_column_to_pairs({col}): {e}')
        return []


# Mapping ENTSO-E verbose column names → compact snake_case keys
_GEN_MAP = {
    'Solar':                          'solar',
    'Wind Onshore':                   'wind_onshore',
    'Wind Offshore':                  'wind_offshore',
    'Nuclear':                        'nuclear',
    'Fossil Brown coal/Lignite':      'lignite',
    'Fossil Hard coal':               'hard_coal',
    'Fossil Gas':                     'natural_gas',
    'Hydro Run-of-river and poundage':'hydro_run_of_river',
    'Hydro Water Reservoir':          'hydro_reservoir',
    'Hydro Pumped Storage':           'hydro_pumped_storage',
    'Biomass':                        'biomass',
    'Other renewable':                'other_renewable',
    'Other':                          'other',
    'Waste':                          'waste',
    'Fossil Oil':                     'oil',
    'Fossil Coal-derived gas':        'coal_gas',
    'Geothermal':                     'geothermal',
    'Marine':                         'marine',
}


def _flatten_df_columns(df):
    """
    ENTSO-E DataFrames may have MultiLevel columns like
    ('Wind Onshore', 'Actual Aggregated') / ('Wind Onshore', 'Actual Consumption').
    We want only 'Actual Aggregated' level. Fall back gracefully.
    """
    if not hasattr(df.columns, 'levels'):
        return df
    try:
        return df['Actual Aggregated']
    except KeyError:
        pass
    # Try selecting by level value
    try:
        mask = df.columns.get_level_values(1) == 'Actual Aggregated'
        sub = df.loc[:, mask]
        sub.columns = sub.columns.get_level_values(0)
        return sub
    except Exception:
        # Last resort: flatten completely, take first level name
        df.columns = [
            str(c[0]) if isinstance(c, tuple) else str(c)
            for c in df.columns
        ]
        return df


# ── Main fetcher ───────────────────────────────────────────────────────────────

def fetch() -> dict:
    """
    Fetch all ENTSO-E data. Each query is wrapped in its own try/except.
    Failures write empty lists/dicts — they never abort the other queries.

    Returns {'data': {...}, 'meta': {...}} — v5 schema required by store.write_with_fallback.
    """
    token = os.environ.get('ENTSOE_SECURITY_TOKEN', '').strip()
    if not token:
        print('  ! ENTSO-E: ENTSOE_SECURITY_TOKEN not set — skipping')
        print('    → Email transparency@entsoe.eu to request API access')
        # Return minimal schema so frontend renders a clear "waiting" state
        return {
            'data': {
                'awaiting_key': True,
                'note': (
                    'Set ENTSOE_SECURITY_TOKEN as a GitHub Secret to enable ENTSO-E data. '
                    'Request access by emailing transparency@entsoe.eu.'
                ),
                'day_ahead_prices': {},
                'generation': {},
                'load': [],
                'wind_solar_forecast': {},
                'crossborder_flows': {},
                'net_position': [],
                'installed_capacity': {},
            },
            'meta': {'source': 'ENTSO-E Transparency Platform', 'note': 'awaiting token'},
        }

    try:
        import pandas as pd
        from entsoe import EntsoePandasClient
        from entsoe.exceptions import NoMatchingDataError
    except ImportError as e:
        print(f'  ! ENTSO-E: missing dependency ({e})')
        print('    → Run: pip install entsoe-py pandas')
        return {
            'data': {
                'awaiting_key': False,
                'error': f'import: {e}',
                'day_ahead_prices': {},
                'generation': {},
                'load': [],
                'wind_solar_forecast': {},
                'crossborder_flows': {},
                'net_position': [],
                'installed_capacity': {},
            },
            'meta': {'source': 'ENTSO-E Transparency Platform', 'note': f'import error: {e}'},
        }

    client = EntsoePandasClient(api_key=token)
    berlin = pytz.timezone('Europe/Berlin')
    now_b = datetime.now(berlin)

    # Time window definitions
    end_now   = pd.Timestamp(now_b).floor('h') + pd.Timedelta(hours=1)
    start_14d = end_now - pd.Timedelta(days=14)
    start_7d  = end_now - pd.Timedelta(days=7)
    start_48h = end_now - pd.Timedelta(hours=48)
    end_fcst  = end_now + pd.Timedelta(hours=48)
    year_start = pd.Timestamp(f'{now_b.year}-01-01', tz=berlin)
    year_end   = pd.Timestamp(f'{now_b.year}-12-31', tz=berlin)

    out = {
        'awaiting_key': False,
        'day_ahead_prices': {},
        'generation': {},
        'load': [],
        'wind_solar_forecast': {},
        'crossborder_flows': {},
        'net_position': [],
        'installed_capacity': {},
    }

    # ── 1. Day-ahead prices (5 countries) ──────────────────────────────────────
    for label, area in [('DE_LU', 'DE_LU'), ('AT', 'AT'), ('FR', 'FR'),
                        ('PL', 'PL'), ('CH', 'CH')]:
        try:
            s = client.query_day_ahead_prices(area, start=start_14d, end=end_now)
            out['day_ahead_prices'][label] = _series_to_pairs(s)
            print(f'    entsoe/price_{label}: {len(out["day_ahead_prices"][label])} pts')
            time.sleep(0.5)
        except NoMatchingDataError:
            print(f'    entsoe/price_{label}: NoMatchingData (area may not publish hourly)')
            out['day_ahead_prices'][label] = []
        except Exception as e:
            print(f'  ! entsoe/price_{label}: {e}')
            out['day_ahead_prices'][label] = []

    # ── 2. Generation mix DE (by fuel type) ────────────────────────────────────
    try:
        df = client.query_generation('DE_LU', start=start_7d, end=end_now)
        df = _flatten_df_columns(df)
        for raw_col in df.columns:
            key = _GEN_MAP.get(str(raw_col),
                               str(raw_col).lower().replace(' ', '_').replace('/', '_'))
            pairs = _df_column_to_pairs(df, raw_col)
            if pairs:
                out['generation'][key] = pairs
        total_pts = sum(len(v) for v in out['generation'].values())
        print(f'    entsoe/generation: {len(out["generation"])} types, {total_pts} total pts')
        time.sleep(0.5)
    except NoMatchingDataError:
        print('    entsoe/generation: NoMatchingData')
    except Exception as e:
        print(f'  ! entsoe/generation: {e}')

    # ── 3. Actual load DE ──────────────────────────────────────────────────────
    try:
        df = client.query_load('DE_LU', start=start_7d, end=end_now)
        col = 'Actual Load' if 'Actual Load' in df.columns else df.columns[0]
        out['load'] = _df_column_to_pairs(df, col)
        print(f'    entsoe/load: {len(out["load"])} pts')
        time.sleep(0.5)
    except NoMatchingDataError:
        print('    entsoe/load: NoMatchingData')
    except Exception as e:
        print(f'  ! entsoe/load: {e}')

    # ── 4. Wind + solar day-ahead forecast DE ─────────────────────────────────
    _FCST_MAP = {'Solar': 'solar', 'Wind Onshore': 'wind_onshore',
                 'Wind Offshore': 'wind_offshore'}
    try:
        df = client.query_wind_and_solar_forecast('DE_LU', start=start_48h, end=end_fcst)
        df = _flatten_df_columns(df)
        for raw_col in df.columns:
            key = _FCST_MAP.get(str(raw_col),
                                str(raw_col).lower().replace(' ', '_'))
            pairs = _df_column_to_pairs(df, raw_col)
            if pairs:
                out['wind_solar_forecast'][key] = pairs
        total_fcst = sum(len(v) for v in out['wind_solar_forecast'].values())
        print(f'    entsoe/wind_solar_forecast: {total_fcst} total pts')
        time.sleep(0.5)
    except NoMatchingDataError:
        print('    entsoe/wind_solar_forecast: NoMatchingData')
    except Exception as e:
        print(f'  ! entsoe/wind_solar_forecast: {e}')

    # ── 5. Cross-border flows (6 pairs × 2 directions = 12 series) ────────────
    _BORDER_PAIRS = [
        ('DE_LU', 'FR'),
        ('DE_LU', 'AT'),
        ('DE_LU', 'PL'),
        ('DE_LU', 'DK_1'),
        ('DE_LU', 'NL'),
        ('DE_LU', 'CZ'),
    ]
    for from_cc, to_cc in _BORDER_PAIRS:
        for a, b in [(from_cc, to_cc), (to_cc, from_cc)]:
            label = f'{a}->{b}'
            try:
                s = client.query_crossborder_flows(a, b, start=start_48h, end=end_now)
                out['crossborder_flows'][label] = _series_to_pairs(s)
                print(f'    entsoe/flow {label}: {len(out["crossborder_flows"][label])} pts')
                time.sleep(0.4)
            except NoMatchingDataError:
                print(f'    entsoe/flow {label}: NoMatchingData')
                out['crossborder_flows'][label] = []
            except Exception as e:
                print(f'  ! entsoe/flow {label}: {e}')
                out['crossborder_flows'][label] = []

    # ── 6. Net position DE (positive = net exporter) ──────────────────────────
    try:
        s = client.query_net_position('DE_LU', start=start_7d, end=end_now, dayahead=True)
        out['net_position'] = _series_to_pairs(s)
        print(f'    entsoe/net_position: {len(out["net_position"])} pts')
        time.sleep(0.5)
    except NoMatchingDataError:
        print('    entsoe/net_position: NoMatchingData')
    except Exception as e:
        print(f'  ! entsoe/net_position: {e}')

    # ── 7. Installed generation capacity DE (latest annual snapshot) ──────────
    try:
        df = client.query_installed_generation_capacity(
            'DE_LU', start=year_start, end=year_end
        )
        df = _flatten_df_columns(df)
        if len(df) > 0:
            latest = df.iloc[-1]
            cap = {}
            for col in latest.index:
                key = _GEN_MAP.get(str(col),
                                   str(col).lower().replace(' ', '_').replace('/', '_'))
                try:
                    v = float(latest[col])
                    if not math.isnan(v):
                        cap[key] = round(v, 1)
                except Exception:
                    pass
            out['installed_capacity'] = cap
            print(f'    entsoe/installed_capacity: {len(cap)} fuel types')
        time.sleep(0.5)
    except NoMatchingDataError:
        print('    entsoe/installed_capacity: NoMatchingData')
    except Exception as e:
        print(f'  ! entsoe/installed_capacity: {e}')

    return {
        'data': out,
        'meta': {
            'source': 'ENTSO-E Transparency Platform',
            'url': 'https://transparency.entsoe.eu',
            'license': 'free with registration (token required)',
            'areas': ['DE_LU', 'AT', 'FR', 'PL', 'CH'],
            'cross_border_pairs': 6,
        },
    }
