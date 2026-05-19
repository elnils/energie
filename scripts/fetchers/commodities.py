"""
Yahoo Finance market data — commodities, energy futures, indices, FX-related.

Strategy:
  - One Yahoo /chart call per ticker for 2y daily history.
  - Quote synthesised from the last two daily closes (avoids the fragile
    v7/quote crumb-cookie auth that 401s from cloud IPs).

Failure handling: per-ticker tolerance, the wrapper marks stale only if
ALL tickers fail.

v5.3 fixes:
  - SXEP.MI replaced with EXH1.DE (iShares STOXX Europe 600 Oil & Gas
    UCITS ETF, XETRA-listed in EUR). The previous SXEP.MI symbol was
    delisted/renamed on Yahoo and returned 404 on every fetch.
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
    # STOXX Europe 600 Oil & Gas: tracked via the iShares UCITS ETF on XETRA,
    # which has a reliable Yahoo quote unlike the direct SXEP index symbol.
    ('stoxx_energy',    'EXH1.DE', 'EUR',          None,                 'indices', 'Stoxx Europe 600 Oil & Gas (ETF EXH1.DE)'),
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


def _synthesize_quote_from_series(series: List[dict]) -> Optional[dict]:
    """Build a {price, change, change_pct, ...} quote from the last two points."""
    if not series:
        return None
    last = series[-1]
    prev = series[-2] if len(series) >= 2 else None
    price = last.get('v')
    if price is None:
        return None
    change = None
    change_pct = None
    if prev is not None and prev.get('v') is not None and prev['v'] != 0:
        change = round(price - prev['v'], 4)
        change_pct = round((price - prev['v']) / prev['v'] * 100, 4)
    return {
        'price':           price,
        'change':          change,
        'change_pct':      change_pct,
        'previous_close':  prev.get('v') if prev else None,
        'time':            last.get('ts'),
        'state':           'EOD',
        'currency':        None,
    }


def fetch() -> dict:
    out: Dict[str, dict] = {}
    success = 0

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
            q = _synthesize_quote_from_series(series)
            if q:
                out[tid]['quote'] = q
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

    if success == 0:
        raise RuntimeError('Yahoo: all commodities failed')

    return {
        'data': out,
        'meta': {
            'source': 'Yahoo Finance (v8 chart endpoint)',
            'license': 'see Yahoo Finance terms; non-commercial display ok',
            'tickers_total': len(TICKERS),
            'tickers_with_history': success,
            'note': 'quote synthesised from last 2 daily closes; v7/quote requires fragile crumb auth.',
        },
    }
