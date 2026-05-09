"""
Yahoo Finance market data — commodities, energy futures, indices, FX-related.

Strategy:
  - One Yahoo /chart call per ticker for 2y daily history.
  - One single /quote batch call for ALL tickers — returns intraday/last
    price (15-minute delayed for futures, real-time for stocks).

Failure handling: per-ticker tolerance, the wrapper marks stale only if
ALL tickers fail. Quote-endpoint failure is non-fatal.
"""
import time
from typing import Dict, List, Optional

from core import http, validators


# ticker_id, yahoo_symbol, unit, validation_metric, category, label
TICKERS = [
    # ── Energie: Crude Oil ──
    ('brent_crude',     'BZ=F',  'USD/Barrel',     'brent_usd_bbl',      'oil',     'Brent Rohöl'),
    ('wti_crude',       'CL=F',  'USD/Barrel',     'wti_usd_bbl',        'oil',     'WTI Crude'),
    # ── Energie: Refined products ──
    ('heating_oil_fut', 'HO=F',  'USD/Gallon',     None,                 'oil',     'Heizöl Future (NY)'),
    ('rbob_gasoline',   'RB=F',  'USD/Gallon',     None,                 'oil',     'Benzin Future (RBOB)'),
    # ── Energie: Natural gas ──
    ('natgas_henry',    'NG=F',  'USD/MMBtu',      'henryhub_usd_mmbtu', 'gas',     'Henry Hub (US)'),
    ('ttf_eu_proxy',    'TTF=F', 'EUR/MWh',        None,                 'gas',     'TTF (EU proxy)'),
    # ── Energie: Coal ──
    ('coal_atw',        'MTF=F', 'USD/t',          None,                 'coal',    'Coal (API2 ARA)'),
    # ── Strom Futures ──
    ('power_de_proxy',  'EBM=F', 'EUR/MWh',        None,                 'power',   'EU Power Future (proxy)'),
    # ── Metalle ──
    ('gold',            'GC=F',  'USD/oz',         'gold_usd_oz',        'metals',  'Gold'),
    ('silver',          'SI=F',  'USD/oz',         None,                 'metals',  'Silber'),
    ('copper',          'HG=F',  'USD/lb',         None,                 'metals',  'Kupfer'),
    ('platinum',        'PL=F',  'USD/oz',         None,                 'metals',  'Platin'),
    ('aluminium_lme',   'ALI=F', 'USD/t',          None,                 'metals',  'Aluminium (LME)'),
    # ── Macro / Volatility ──
    ('vix',             '^VIX',  'index',          None,                 'macro',   'VIX (Volatilität S&P)'),
    ('dxy',             'DX-Y.NYB', 'index',       None,                 'macro',   'USD Index (DXY)'),
    ('us_10y',          '^TNX',  '%',              None,                 'macro',   'US 10Y Treasury'),
    # ── Aktien-Indizes ──
    ('dax',             '^GDAXI','index',          None,                 'indices', 'DAX 40'),
    ('sp500',           '^GSPC', 'index',          None,                 'indices', 'S&P 500'),
    ('stoxx_europe',    '^STOXX','index',          None,                 'indices', 'Stoxx Europe 600'),
    ('stoxx_energy',    'SXEP.MI', 'index',        None,                 'indices', 'Stoxx Europe Energy (proxy)'),
    # ── Energie-bezogene ETFs/Aktien ──
    ('uranium_url',     'URA',   'USD',            None,                 'energy_eq', 'Uranium ETF (URA)'),
    ('lithium_lit',     'LIT',   'USD',            None,                 'energy_eq', 'Lithium ETF (LIT)'),
    ('clean_energy',    'ICLN',  'USD',            None,                 'energy_eq', 'Clean Energy ETF (ICLN)'),
    # ── Crypto ──
    ('bitcoin',         'BTC-USD','USD',           None,                 'macro',   'Bitcoin'),
]


def _yahoo_chart(ticker: str, range_str: str = '2y', interval: str = '1d') -> Optional[List[dict]]:
    s = http.get_session()
    enc = ticker.replace('=', '%3D').replace('^', '%5E')
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{enc}'
    r = s.get(url, params={'range': range_str, 'interval': interval},
              timeout=20,
              headers={
                  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                  'Referer': 'https://finance.yahoo.com',
                  'Accept': 'application/json',
              })
    r.raise_for_status()
    payload = r.json()
    result = payload.get('chart', {}).get('result')
    if not result:
        return None
    res = result[0]
    ts = res.get('timestamp') or []
    quote = res.get('indicators', {}).get('quote', [{}])[0]
    closes = quote.get('close', [])
    series = []
    for t, c in zip(ts, closes):
        if c is None:
            continue
        series.append({'ts': t, 'v': round(float(c), 4)})
    return series


def _yahoo_quote_batch(tickers: List[str]) -> Dict[str, dict]:
    if not tickers:
        return {}
    s = http.get_session()
    symbols = ','.join(tickers)
    url = 'https://query1.finance.yahoo.com/v7/finance/quote'
    try:
        r = s.get(url, params={'symbols': symbols}, timeout=15,
                  headers={
                      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                      'Referer': 'https://finance.yahoo.com',
                      'Accept': 'application/json',
                  })
        r.raise_for_status()
        results = r.json().get('quoteResponse', {}).get('result', [])
        return {q['symbol']: q for q in results if 'symbol' in q}
    except Exception as e:
        print(f'  ! quote batch failed: {e}')
        return {}


def fetch() -> dict:
    out: Dict[str, dict] = {}
    success = 0

    # 1) Pull historical series per ticker
    for tid, symbol, unit, metric, category, label in TICKERS:
        try:
            series = _yahoo_chart(symbol)
            if series is None:
                series = []
            if metric and series:
                series = [p for p in series if validators.in_range(metric, p['v'])]
            out[tid] = {
                'unit': unit,
                'symbol': symbol,
                'category': category,
                'label': label,
                'series': series,
            }
            if series:
                success += 1
                print(f'    commodity/{tid} ({symbol}): {len(series)} pts')
            else:
                print(f'    commodity/{tid} ({symbol}): no data')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! commodity/{tid} ({symbol}): {str(e)[:120]}')
            out[tid] = {
                'unit': unit, 'symbol': symbol, 'category': category,
                'label': label, 'series': [],
            }

    # 2) One batched call for live quotes
    quotes = _yahoo_quote_batch([t[1] for t in TICKERS])
    quotes_count = 0
    for tid, symbol, *_ in TICKERS:
        q = quotes.get(symbol)
        if q:
            out[tid]['quote'] = {
                'price':           q.get('regularMarketPrice'),
                'change':          q.get('regularMarketChange'),
                'change_pct':      q.get('regularMarketChangePercent'),
                'previous_close':  q.get('regularMarketPreviousClose'),
                'time':            q.get('regularMarketTime'),
                'state':           q.get('marketState'),
                'currency':        q.get('currency'),
            }
            quotes_count += 1
    print(f'    commodity/quotes: {quotes_count}/{len(TICKERS)} live prices')

    if success == 0:
        raise RuntimeError('Yahoo: all commodities failed')

    # Schema-friendly: brent_crude must remain a top-level key for the
    # store.py validator. Other tickers stay alongside it.
    return {
        'data': out,
        'meta': {
            'source': 'Yahoo Finance (chart + quote endpoints)',
            'license': 'see Yahoo Finance terms; non-commercial display ok',
            'tickers_total': len(TICKERS),
            'tickers_with_history': success,
            'tickers_with_live_quote': quotes_count,
        },
    }
