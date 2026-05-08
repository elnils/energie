"""
Yahoo Finance commodities (Brent, WTI, Henry Hub, Gold, Coal).

Yahoo intermittently blocks GitHub-Actions outbound IPs. We tolerate
individual ticker failures via the wrapper. If ALL fail, we raise so the
fallback keeps last good values.
"""
import time
from typing import Dict, List

from core import http, validators


TICKERS = {
    'brent_crude':  ('BZ=F', 'USD/Barrel',  'brent_usd_bbl'),
    'wti_crude':    ('CL=F', 'USD/Barrel',  'wti_usd_bbl'),
    'natgas_henry': ('NG=F', 'USD/MMBtu',   'henryhub_usd_mmbtu'),
    'gold':         ('GC=F', 'USD/oz',      'gold_usd_oz'),
    'coal':         ('MTF=F', 'USD/t',      None),  # no range configured
}


def _fetch_one(ticker: str, metric: str | None) -> List[dict]:
    s = http.get_session()
    enc = ticker.replace('=', '%3D')
    r = s.get(
        f'https://query1.finance.yahoo.com/v8/finance/chart/{enc}?range=2y&interval=1d',
        timeout=20,
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Referer': 'https://finance.yahoo.com',
        },
    )
    r.raise_for_status()
    res = r.json()['chart']['result'][0]
    ts = res['timestamp']
    closes = res['indicators']['quote'][0]['close']
    series = []
    for t, c in zip(ts, closes):
        if c is None:
            continue
        if metric and not validators.in_range(metric, c):
            continue
        series.append({'ts': t, 'v': round(float(c), 4)})
    return series


def fetch() -> dict:
    out: Dict[str, dict] = {}
    success = 0
    for name, (ticker, unit, metric) in TICKERS.items():
        try:
            series = _fetch_one(ticker, metric)
            out[name] = {'unit': unit, 'series': series}
            print(f'    commodity/{name}: {len(series)} pts')
            if series:
                success += 1
            time.sleep(0.4)
        except Exception as e:
            print(f'  ! commodity/{name}: {e}')
            out[name] = {'unit': unit, 'series': []}

    if success == 0:
        raise RuntimeError('Yahoo: all commodities failed')

    return {
        'data': out,
        'meta': {
            'source': 'Yahoo Finance',
            'license': 'see Yahoo Finance terms; non-commercial display ok',
        },
    }
