"""
Energy Futures & Price Forecasts

Interval: 360 min (6h) — prices change slowly, forecasts update monthly.

Sources:
  1. EIA STEO API  — Brent/WTI/EU Gas/Henry Hub monatlich, 24+ Monate voraus
                     Zukünftige Monate = offizielle EIA Prognose
  2. Yahoo Finance  — Spot-Preise (direkt via v8/finance/chart, kein yfinance)
  3. World Bank API — Jahres-Prognosen (kein Key)
  4. IMF DataMapper — Jahres-Prognosen (kein Key)
  5. JSONL Snapshot — wöchentlicher Kurvenvergleich (Vorwoche vs. heute)
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

from core import http, paths, history


HIST_FILE = os.path.join(paths.DATA_DIR, 'history', 'futures.jsonl')

EIA_STEO_SERIES = {
    'brent':  'BREPUUS',    # Brent crude oil spot price    USD/bbl
    'wti':    'WTIPUUS',    # WTI crude oil spot price      USD/bbl
    'eu_gas': 'NGEUPUUS',   # European natural gas (TTF)   USD/MMBtu
    'hh_gas': 'NGHHUUS',    # Henry Hub natural gas         USD/MMBtu
}

SPOT_TICKERS = [
    ('brent_usd_bbl',    'BZ%3DF',  'USD/bbl'),
    ('wti_usd_bbl',      'CL%3DF',  'USD/bbl'),
    ('ttf_usd_mmbtu',    'TTF%3DF', 'USD/MMBtu'),
    ('natgas_usd_mmbtu', 'NG%3DF',  'USD/MMBtu'),
    ('heating_usd_gal',  'HO%3DF',  'USD/gal'),
    ('gasoil_usd_t',     'QS%3DF',  'USD/MT'),
]

WB_INDICATORS  = {'crude_oil': 'POILAPSP', 'eu_gas': 'PNGASEU'}
IMF_INDICATORS = {'crude_oil': 'POILAPSP', 'eu_gas': 'PNGASEU'}


# ── EIA STEO ──────────────────────────────────────────────────────────────────

def _fetch_eia_steo(api_key: str) -> dict:
    s = http.get_session()
    now = datetime.now(timezone.utc)
    forecast_from = f"{now.year}-{now.month:02d}"
    result = {'forecast_from': forecast_from}

    for key, series_id in EIA_STEO_SERIES.items():
        try:
            r = s.get(
                'https://api.eia.gov/v2/steo/data/',
                params=[
                    ('api_key', api_key),
                    ('frequency', 'monthly'),
                    ('data[0]', 'value'),
                    ('facets[seriesId][]', series_id),
                    ('sort[0][column]', 'period'),
                    ('sort[0][direction]', 'desc'),   # desc = newest first → includes future months
                    ('offset', '0'),
                    ('length', '36'),                 # 36 months: ~12 actual + ~24 forecast
                ],
                timeout=30,
            )
            if not r.ok:
                print(f'  ! eia_steo/{key}: HTTP {r.status_code}')
                result[key] = []
                continue
            rows = r.json().get('response', {}).get('data', [])
            series = []
            for row in rows:
                period = row.get('period', '')
                val    = row.get('value')
                if not period or val is None:
                    continue
                try:
                    v = round(float(val), 3)
                except (TypeError, ValueError):
                    continue
                ptype = 'forecast' if period >= forecast_from else 'actual'
                series.append({'period': period, 'v': v, 'type': ptype})
            result[key] = series
            n_fc = sum(1 for x in series if x['type'] == 'forecast')
            n_ac = len(series) - n_fc
            print(f'    eia_steo/{key}: {n_ac} actual + {n_fc} forecast')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! eia_steo/{key}: {e}')
            result[key] = []

    return result


# ── Yahoo Finance spot ─────────────────────────────────────────────────────────

def _fetch_yahoo_spot() -> dict:
    s = http.get_session()
    yh_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Referer': 'https://finance.yahoo.com',
    }
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    spot = {}

    for key, enc, unit in SPOT_TICKERS:
        try:
            r = s.get(
                f'https://query1.finance.yahoo.com/v8/finance/chart/{enc}'
                f'?range=5d&interval=1d',
                timeout=20,
                headers=yh_headers,
            )
            if not r.ok:
                continue
            res = r.json()['chart']['result'][0]
            closes = [c for c in res['indicators']['quote'][0]['close'] if c is not None]
            if closes:
                spot[key] = {
                    'price':  round(closes[-1], 4),
                    'unit':   unit,
                    'date':   today,
                    'source': 'Yahoo Finance',
                }
            time.sleep(0.2)
        except Exception as e:
            print(f'  ! yahoo_spot/{key}: {e}')

    # Derived: TTF USD/MMBtu → EUR/MWh (approx ÷3.41 × EUR/USD ~0.92)
    if 'ttf_usd_mmbtu' in spot:
        v = spot['ttf_usd_mmbtu']['price']
        spot['ttf_eur_mwh'] = {
            'price':  round(v / 3.41 * 0.92, 2),
            'unit':   'EUR/MWh (approx)',
            'date':   today,
            'source': 'Yahoo Finance TTF=F (converted)',
        }

    print(f'    yahoo_spot: {len(spot)} prices')
    return spot


# ── World Bank ─────────────────────────────────────────────────────────────────

def _fetch_worldbank() -> dict:
    s = http.get_session()
    cur_year = datetime.now(timezone.utc).year
    result = {}

    for key, ind in WB_INDICATORS.items():
        try:
            r = s.get(
                f'https://api.worldbank.org/v2/country/WLD/indicator/{ind}'
                f'?format=json&per_page=15&date={cur_year-4}:{cur_year+3}',
                timeout=20,
            )
            if not r.ok:
                result[key] = []
                continue
            payload = r.json()
            # World Bank returns [{pages_meta}, [data_rows]] or a single dict on error
            rows = []
            if isinstance(payload, list) and len(payload) > 1:
                rows = payload[1] or []
            elif isinstance(payload, dict):
                rows = payload.get('value', payload.get('data', []))
            series = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                try:
                    yr  = int(row.get('date', '0'))
                    val = row.get('value')
                    if val is not None and yr >= 2018:
                        series.append({'year': yr, 'v': round(float(val), 3)})
                except (ValueError, TypeError):
                    pass
            series.sort(key=lambda x: x['year'])
            result[key] = series
            print(f'    worldbank/{key}: {len(series)} years ({[x["year"] for x in series]})')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! worldbank/{key}: {e}')
            result[key] = []

    result['source'] = 'World Bank (no auth required)'
    return result


# ── IMF DataMapper ─────────────────────────────────────────────────────────────

def _fetch_imf() -> dict:
    s = http.get_session()
    result = {}

    for key, ind in IMF_INDICATORS.items():
        try:
            r = s.get(
                f'https://www.imf.org/external/datamapper/api/v1/{ind}',
                timeout=20,
            )
            if not r.ok:
                result[key] = []
                continue
            payload  = r.json()
            ind_data = payload.get('values', {}).get(ind, {})
            # Find the global aggregate key — IMF uses various codes
            vals = {}
            for candidate in ('WORLD', 'WLD', '001', 'W00', ''):
                if candidate in ind_data and ind_data[candidate]:
                    vals = ind_data[candidate]
                    break
            if not vals:
                # Fall back: take entry with most data points
                best_len = 0
                for v in ind_data.values():
                    if isinstance(v, dict) and len(v) > best_len:
                        best_len = len(v)
                        vals = v
            series = []
            for yr_str, val in vals.items():
                try:
                    yr = int(yr_str)
                    if 2018 <= yr <= 2030 and val is not None:
                        series.append({'year': yr, 'v': round(float(val), 3)})
                except (ValueError, TypeError):
                    pass
            series.sort(key=lambda x: x['year'])
            result[key] = series
            print(f'    imf/{key}: {len(series)} years')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! imf/{key}: {e}')
            result[key] = []

    result['source'] = 'IMF World Economic Outlook (no auth required)'
    return result


# ── History snapshot ──────────────────────────────────────────────────────────

def _save_snapshot(eia: dict) -> None:
    os.makedirs(os.path.dirname(HIST_FILE), exist_ok=True)
    record = {
        'date':       datetime.now(timezone.utc).strftime('%Y-%m-%d'),
        'eia_brent':  eia.get('brent',  []),
        'eia_wti':    eia.get('wti',    []),
        'eia_eu_gas': eia.get('eu_gas', []),
        'eia_hh_gas': eia.get('hh_gas', []),
    }
    try:
        with open(HIST_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
    except Exception as e:
        print(f'  ! futures snapshot write: {e}')


def _load_previous_week() -> Optional[dict]:
    if not os.path.exists(HIST_FILE):
        return None
    target = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d')
    best, best_delta = None, 999
    try:
        with open(HIST_FILE, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec   = json.loads(line)
                    rec_d = rec.get('date', '')
                    if rec_d:
                        delta = abs(
                            (datetime.strptime(rec_d, '%Y-%m-%d')
                             - datetime.strptime(target, '%Y-%m-%d')).days
                        )
                        if delta < best_delta:
                            best_delta = delta
                            best = rec
                except Exception:
                    pass
    except Exception as e:
        print(f'  ! futures history read: {e}')
    if best and best_delta <= 10:
        print(f'    futures history: {best["date"]} (Δ{best_delta}d ago)')
        return best
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def fetch() -> dict:
    eia_key = os.environ.get('EIA_API_KEY', '').strip()
    today   = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # 1. EIA STEO
    if eia_key:
        eia_steo = _fetch_eia_steo(eia_key)
    else:
        print('  ! energy_futures: EIA_API_KEY not set — STEO skipped')
        eia_steo = {k: [] for k in list(EIA_STEO_SERIES) + ['forecast_from']}
        eia_steo['forecast_from'] = datetime.now(timezone.utc).strftime('%Y-%m')

    # 2. Yahoo spot
    spot = _fetch_yahoo_spot()

    # 3. World Bank (no auth)
    worldbank = _fetch_worldbank()

    # 4. IMF (no auth)
    imf = _fetch_imf()

    # 5. History
    if any(eia_steo.get(k) for k in EIA_STEO_SERIES):
        _save_snapshot(eia_steo)
    previous_week = _load_previous_week()

    return {
        'data': {
            'spot':             spot,
            'eia_steo':         eia_steo,
            'worldbank':        worldbank,
            'imf':              imf,
            'history_snapshot': {
                'date':       today,
                'eia_brent':  eia_steo.get('brent',  []),
                'eia_wti':    eia_steo.get('wti',    []),
                'eia_eu_gas': eia_steo.get('eu_gas', []),
                'eia_hh_gas': eia_steo.get('hh_gas', []),
            },
            'previous_week': previous_week,
        },
        'meta': {
            'source':   'EIA STEO + Yahoo Finance + World Bank + IMF',
            'interval': 360,
            'note': (
                'EIA STEO: future months = official EIA price forecast (monthly update). '
                'World Bank + IMF: annual projections, no auth required. '
                'previous_week: JSONL snapshot for week-over-week curve comparison.'
            ),
        },
    }
