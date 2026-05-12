"""
Energy Futures & Price Forecasts

Datenquellen:
  1. EIA STEO API     — monatliche Preis-Forecasts bis ~24 Monate voraus
                        Brent (BREPUUS), WTI (WTIPUUS), EU Gas (NGTEIUUS), HH (NGHNGUS)
                        Zukünftige Monate = offizielle EIA-Prognose
  2. Yahoo Finance    — aktueller Spot + Monats-Futures-Kontrakte (near curve)
                        BZ=F, BZM26.NYM...  TTF=F, NG=F
  3. World Bank API   — Jahres-Prognosen (POILAPSP, PNGASEU)
  4. IMF DataMapper   — Jahres-Prognosen Energie-Rohstoffe
  5. History Snapshot — jeder Run speichert aktuelle Kurve in JSONL
                        → Frontend zeigt "aktuelle Woche vs. Vorwoche"

Output: data/energy_futures.json
{
  "updated": "...",
  "spot": {
    "brent_usd_bbl": {"price": 82.5, "date": "2026-05-12", "source": "Yahoo"},
    "ttf_eur_mwh":   {"price": 35.1, "date": "2026-05-12", "source": "Yahoo"},
    "ttf_usd_mmbtu": {"price": 10.3, "date": "2026-05-12", "source": "Yahoo"},
    "natgas_usd_mmbtu": {"price": 2.1, "date": "2026-05-12", "source": "Yahoo"},
    "wti_usd_bbl":   {"price": 79.2, "date": "2026-05-12", "source": "Yahoo"},
  },
  "eia_steo": {
    "brent":   [{"period":"2026-01","v":82.5,"type":"actual"},
                {"period":"2026-06","v":80.0,"type":"forecast"},...],
    "eu_gas":  [...],   // USD/MMBtu European gas import price
    "hh_gas":  [...],   // USD/MMBtu Henry Hub
    "wti":     [...],
    "forecast_months": "2026-06"   // first forecast month
  },
  "yahoo_futures": {
    "brent": [{"expiry":"2026-07","symbol":"BZN26.NYM","price":81.5},
              {"expiry":"2026-12","symbol":"BZZ26.NYM","price":79.0},...],
    "natgas":[...],
  },
  "worldbank": {
    "crude_oil": [{"year":2024,"v":83.0},{"year":2025,"v":78.0},...],
    "eu_gas":    [{"year":2024,"v":12.5},{"year":2025,"v":10.0},...],
    "source": "World Bank Commodity Markets Outlook"
  },
  "imf": {
    "crude_oil": [{"year":2025,"v":78.5},{"year":2026,"v":72.0},...],
    "eu_gas":    [...],
    "source": "IMF World Economic Outlook"
  },
  "history_snapshot": {   // today's curves, also written to JSONL
    "date": "2026-05-12",
    "eia_brent": [...],
    "eia_eu_gas": [...]
  },
  "previous_week": {    // loaded from JSONL history (7 days ago)
    "date": "2026-05-05",
    "eia_brent": [...],
    "eia_eu_gas": [...]
  }
}
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from core import http, paths


DATA_DIR   = paths.DATA_DIR
HIST_FILE  = os.path.join(DATA_DIR, 'history', 'futures.jsonl')


# ── Futures contract month codes (standard CME/ICE convention) ───────────────
MONTH_CODES = {
    1:'F', 2:'G', 3:'H', 4:'J', 5:'K', 6:'M',
    7:'N', 8:'Q', 9:'U', 10:'V', 11:'X', 12:'Z'
}
MONTH_NAMES = {v:k for k,v in MONTH_CODES.items()}


def _period_to_date(period: str) -> str:
    """'2026-05' → '2026-05-01'"""
    return period + '-01' if len(period) == 7 else period


# ── 1. EIA STEO ──────────────────────────────────────────────────────────────

EIA_STEO_SERIES = {
    'brent':  'BREPUUS',    # Brent crude USD/bbl
    'wti':    'WTIPUUS',    # WTI crude USD/bbl
    'eu_gas': 'NGTEIUUS',   # European natural gas import price USD/MMBtu
    'hh_gas': 'NGHNGUS',    # Henry Hub natural gas USD/MMBtu
}


def _fetch_eia_steo(api_key: str) -> dict:
    """
    Fetch EIA STEO monthly price data for all energy series.
    Returns {series_key: [{period, v, type}]}, where type='actual'|'forecast'.
    The forecast cutoff = first future period (next month).
    """
    s = http.get_session()
    result = {}
    now = datetime.now(timezone.utc)
    forecast_start = f"{now.year}-{now.month:02d}"  # current month onwards = forecast

    for key, series_id in EIA_STEO_SERIES.items():
        try:
            params = [
                ('api_key', api_key),
                ('frequency', 'monthly'),
                ('data[0]', 'value'),
                ('facets[seriesId][]', series_id),
                ('sort[0][column]', 'period'),
                ('sort[0][direction]', 'asc'),
                ('offset', '0'),
                ('length', '48'),   # 4 years: ~12 hist + ~24-36 forecast
            ]
            r = s.get('https://api.eia.gov/v2/steo/data/', params=params, timeout=30)
            if not r.ok:
                print(f'  ! eia_steo/{key}: HTTP {r.status_code}')
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
                ptype = 'forecast' if period >= forecast_start else 'actual'
                series.append({'period': period, 'v': v, 'type': ptype})
            result[key] = series
            n_fc = sum(1 for r in series if r['type'] == 'forecast')
            n_ac = sum(1 for r in series if r['type'] == 'actual')
            print(f'    eia_steo/{key}: {n_ac} actual + {n_fc} forecast months')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! eia_steo/{key}: {e}')
            result[key] = []

    # Mark first forecast month
    first_fc = forecast_start
    result['forecast_from'] = first_fc
    return result


# ── 2. Yahoo Finance Futures Chain ───────────────────────────────────────────

def _build_futures_symbols(prefix: str, exchange: str,
                            months_ahead: int = 24) -> List[dict]:
    """
    Build list of monthly futures contract symbols for Yahoo Finance.
    e.g. prefix='BZ', exchange='NYM' → BZM26.NYM, BZN26.NYM, ...
    """
    now = datetime.now(timezone.utc)
    symbols = []
    for delta in range(1, months_ahead + 1):
        dt = now + timedelta(days=delta * 30)
        mc = MONTH_CODES[dt.month]
        yy = str(dt.year)[-2:]
        symbol = f'{prefix}{mc}{yy}.{exchange}'
        expiry = f'{dt.year}-{dt.month:02d}'
        symbols.append({'symbol': symbol, 'expiry': expiry})
    return symbols


def _fetch_yahoo_futures(product: str, symbol_list: List[dict]) -> List[dict]:
    """Fetch prices for a list of futures contract symbols via yfinance."""
    try:
        import yfinance as yf
    except ImportError:
        print(f'  ! yahoo_futures/{product}: yfinance not installed')
        return []

    results = []
    for item in symbol_list:
        try:
            t = yf.Ticker(item['symbol'])
            info = t.info or {}
            price = info.get('regularMarketPrice') or info.get('previousClose')
            if price and price > 0:
                results.append({
                    'expiry':  item['expiry'],
                    'symbol':  item['symbol'],
                    'price':   round(float(price), 3),
                })
            time.sleep(0.1)
        except Exception:
            pass   # most symbols won't exist — that's expected

    results.sort(key=lambda x: x['expiry'])
    print(f'    yahoo_futures/{product}: {len(results)} contracts found')
    return results


def _fetch_yahoo_spot() -> dict:
    """Fetch current spot prices for energy front-month contracts."""
    try:
        import yfinance as yf
    except ImportError:
        return {}

    spots = {}
    mappings = [
        ('brent_usd_bbl',    'BZ=F',   'USD/bbl'),
        ('wti_usd_bbl',      'CL=F',   'USD/bbl'),
        ('ttf_usd_mmbtu',    'TTF=F',  'USD/MMBtu'),
        ('natgas_usd_mmbtu', 'NG=F',   'USD/MMBtu'),
        ('heating_usd_gal',  'HO=F',   'USD/gal'),
        ('gasoline_usd_gal', 'RB=F',   'USD/gal'),
        ('gasoil_usd_t',     'QS=F',   'USD/MT'),
    ]
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    for key, symbol, unit in mappings:
        try:
            t = yf.Ticker(symbol)
            info = t.info or {}
            price = info.get('regularMarketPrice') or info.get('previousClose')
            if price and price > 0:
                spots[key] = {'price': round(float(price), 3), 'unit': unit,
                              'symbol': symbol, 'date': today, 'source': 'Yahoo Finance'}
            time.sleep(0.1)
        except Exception as e:
            print(f'  ! yahoo_spot/{key} ({symbol}): {e}')

    # TTF: Yahoo gives USD/MMBtu, convert to EUR/MWh (approx: 1 MMBtu ≈ 0.293 MWh, use USD→EUR ~0.92)
    if 'ttf_usd_mmbtu' in spots:
        ttf_usd = spots['ttf_usd_mmbtu']['price']
        # USD/MMBtu → EUR/MWh: divide by 3.41 (MMBtu/MWh), then multiply by EUR/USD rate (~0.92)
        # This is approximate. EIA gives the real conversion.
        ttf_eur_mwh = round(ttf_usd / 3.41 * 0.92, 2)
        spots['ttf_eur_mwh'] = {
            'price': ttf_eur_mwh, 'unit': 'EUR/MWh',
            'symbol': 'TTF=F (converted)', 'date': today,
            'source': 'Yahoo Finance (USD/MMBtu ÷ 3.41 × 0.92)',
            'note': 'Approximate. Use EIA STEO for official forecast.'
        }

    print(f'    yahoo_spot: {len(spots)} prices fetched')
    return spots


# ── 3. World Bank Commodity Forecasts ────────────────────────────────────────

# Indicator codes:
#   PNGASEU: Natural gas, Europe
#   POILAPSP: Crude oil, avg spot price (Brent/WTI/Dubai)
#   PNGAS: Natural gas index

WB_INDICATORS = {
    'crude_oil': 'POILAPSP',
    'eu_gas':    'PNGASEU',
}


def _fetch_worldbank_forecasts() -> dict:
    """
    World Bank Commodity Price forecast from their DataBank API.
    Includes historical + projected annual values.
    """
    s = http.get_session()
    result = {}
    current_year = datetime.now(timezone.utc).year

    for key, indicator in WB_INDICATORS.items():
        try:
            url = (f'https://api.worldbank.org/v2/country/WLD/indicator/{indicator}'
                   f'?format=json&per_page=15&mrv=10&date={current_year-5}:{current_year+3}')
            r = s.get(url, timeout=20)
            if not r.ok:
                print(f'  ! worldbank/{key}: HTTP {r.status_code}')
                continue
            payload = r.json()
            if not isinstance(payload, list) or len(payload) < 2:
                continue
            data_rows = payload[1]
            series = []
            for row in (data_rows or []):
                year_str = row.get('date', '')
                val = row.get('value')
                if year_str and val is not None:
                    try:
                        series.append({'year': int(year_str), 'v': round(float(val), 3)})
                    except (ValueError, TypeError):
                        pass
            series.sort(key=lambda x: x['year'])
            result[key] = series
            print(f'    worldbank/{key}: {len(series)} years')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! worldbank/{key}: {e}')
            result[key] = []

    result['source'] = 'World Bank Development Data'
    result['note'] = 'Historical USD/bbl (oil) and USD/MMBtu (gas). Future years are WB projections.'
    return result


# ── 4. IMF DataMapper Forecasts ───────────────────────────────────────────────

IMF_INDICATORS = {
    'crude_oil': 'POILAPSP',    # Crude oil price (USD/bbl)
    'eu_gas':    'PNGASEU',     # European gas price (USD/MMBtu)
}


def _fetch_imf_forecasts() -> dict:
    """IMF DataMapper API — annual commodity price forecasts."""
    s = http.get_session()
    result = {}

    for key, indicator in IMF_INDICATORS.items():
        try:
            r = s.get(
                f'https://www.imf.org/external/datamapper/api/v1/{indicator}',
                timeout=20
            )
            if not r.ok:
                print(f'  ! imf/{key}: HTTP {r.status_code}')
                continue
            payload = r.json()
            # IMF returns {values: {INDICATOR: {COUNTRY_OR_WORLD: {year: value}}}}
            vals = (payload.get('values', {}).get(indicator, {}).get('WORLD', {}) or
                    payload.get('values', {}).get(indicator, {}))
            if not vals:
                # Try nested structure
                for country_data in payload.get('values', {}).get(indicator, {}).values():
                    if isinstance(country_data, dict) and country_data:
                        vals = country_data
                        break
            if not vals:
                print(f'  ! imf/{key}: no values in response')
                continue
            series = []
            current_year = datetime.now(timezone.utc).year
            for year_str, val in vals.items():
                try:
                    year = int(year_str)
                    if year < 2018 or year > 2030:
                        continue
                    if val is not None:
                        series.append({'year': year, 'v': round(float(val), 3)})
                except (ValueError, TypeError):
                    pass
            series.sort(key=lambda x: x['year'])
            result[key] = series
            print(f'    imf/{key}: {len(series)} years')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! imf/{key}: {e}')
            result[key] = []

    result['source'] = 'IMF World Economic Outlook'
    return result


# ── 5. History snapshot ──────────────────────────────────────────────────────

def _save_snapshot(eia_steo: dict) -> None:
    """Append today's forward curve to history JSONL."""
    os.makedirs(os.path.dirname(HIST_FILE), exist_ok=True)
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    record = {
        'date':        today,
        'eia_brent':   eia_steo.get('brent',  []),
        'eia_wti':     eia_steo.get('wti',    []),
        'eia_eu_gas':  eia_steo.get('eu_gas', []),
        'eia_hh_gas':  eia_steo.get('hh_gas', []),
    }
    try:
        with open(HIST_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
    except Exception as e:
        print(f'  ! futures snapshot write: {e}')


def _load_previous_week_snapshot() -> Optional[dict]:
    """Load the snapshot from ~7 days ago."""
    if not os.path.exists(HIST_FILE):
        return None
    target = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d')
    best_record = None
    best_delta = 999
    try:
        with open(HIST_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    rec_date = rec.get('date', '')
                    if rec_date:
                        delta = abs((datetime.strptime(rec_date, '%Y-%m-%d')
                                    - datetime.strptime(target, '%Y-%m-%d')).days)
                        if delta < best_delta:
                            best_delta = delta
                            best_record = rec
                except Exception:
                    pass
    except Exception as e:
        print(f'  ! futures history read: {e}')
    if best_record and best_delta <= 10:
        print(f'    futures history: found snapshot from {best_record["date"]} (Δ{best_delta}d)')
        return best_record
    return None


# ── Main fetcher ──────────────────────────────────────────────────────────────

def fetch() -> dict:
    eia_key = os.environ.get('EIA_API_KEY', '').strip()

    # 1. EIA STEO (primary forecast source)
    eia_steo: dict = {}
    if eia_key:
        try:
            eia_steo = _fetch_eia_steo(eia_key)
        except Exception as e:
            print(f'  ! eia_steo: {e}')
    else:
        print('  ! energy_futures: EIA_API_KEY not set — STEO forecasts unavailable')

    # 2. Yahoo Finance spot + futures chain
    spot = {}
    yahoo_futures: dict = {'brent': [], 'natgas': [], 'wti': []}
    try:
        spot = _fetch_yahoo_spot()
        brent_syms  = _build_futures_symbols('BZ', 'NYM', months_ahead=24)
        natgas_syms = _build_futures_symbols('NG', 'NYM', months_ahead=24)
        yahoo_futures['brent']  = _fetch_yahoo_futures('brent', brent_syms)
        yahoo_futures['natgas'] = _fetch_yahoo_futures('natgas', natgas_syms)
    except Exception as e:
        print(f'  ! yahoo_futures: {e}')

    # 3. World Bank
    worldbank: dict = {}
    try:
        worldbank = _fetch_worldbank_forecasts()
    except Exception as e:
        print(f'  ! worldbank: {e}')

    # 4. IMF
    imf: dict = {}
    try:
        imf = _fetch_imf_forecasts()
    except Exception as e:
        print(f'  ! imf: {e}')

    # 5. History
    today_snapshot = {
        'date':       datetime.now(timezone.utc).strftime('%Y-%m-%d'),
        'eia_brent':  eia_steo.get('brent', []),
        'eia_wti':    eia_steo.get('wti', []),
        'eia_eu_gas': eia_steo.get('eu_gas', []),
        'eia_hh_gas': eia_steo.get('hh_gas', []),
    }
    if eia_steo.get('brent'):
        _save_snapshot(eia_steo)
    previous_week = _load_previous_week_snapshot()

    return {
        'data': {
            'spot':              spot,
            'eia_steo':          eia_steo,
            'yahoo_futures':     yahoo_futures,
            'worldbank':         worldbank,
            'imf':               imf,
            'history_snapshot':  today_snapshot,
            'previous_week':     previous_week,
        },
        'meta': {
            'source':     'EIA STEO + Yahoo Finance + World Bank + IMF',
            'eia_series': EIA_STEO_SERIES,
            'note': (
                'EIA STEO: zukuenftige Monate = offizielle EIA Preisprognose (monatlich). '
                'World Bank/IMF: jaehrliche Prognosen (grobgranular). '
                'Yahoo Finance: tagesaktuelle Futures-Kontrakte. '
                'previous_week: gespeicherter Snapshot von ~7 Tagen zuvor fuer Kurvenvergleich.'
            ),
        },
    }
