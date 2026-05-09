"""
ECB euro foreign exchange reference rates.

Endpoints (XML, no auth):
  - eurofxref-daily.xml      latest day, ~30 currencies vs EUR
  - eurofxref-hist-90d.xml   trailing 90 trading days

Update window: working days, ~16:00 CET. Returns nothing on weekends/holidays.
We pull both endpoints — `current` for KPI tiles, `series_90d` for chart.
"""
import xml.etree.ElementTree as ET
from typing import Dict, List
from datetime import datetime

from core import http, validators, history


URL_DAILY = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml'
URL_HIST_90D = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist-90d.xml'

NS = {
    'gesmes': 'http://www.gesmes.org/xml/2002-08-01',
    'ecb':    'http://www.ecb.int/vocabulary/2002-08-01/eurofxref',
}

# Currencies we surface in the dashboard. Anything else stays in `all_rates`.
PRIORITY = ['USD', 'GBP', 'CHF', 'JPY', 'CNY', 'PLN', 'NOK', 'SEK', 'CZK']


def _parse_day_node(day_node) -> dict:
    """Extract {date, rates} from one <Cube time='...'> node."""
    rates: Dict[str, float] = {}
    for c in day_node.findall('ecb:Cube', NS):
        ccy = c.get('currency')
        try:
            rate = float(c.get('rate'))
        except (TypeError, ValueError):
            continue
        rates[ccy] = rate
    return {'date': day_node.get('time'), 'rates': rates}


def _validate_rates(rates: Dict[str, float]) -> Dict[str, float]:
    """Drop rates that fail their range check. Keep what's valid."""
    out: Dict[str, float] = {}
    for ccy, rate in rates.items():
        metric = f'fx_eur_{ccy.lower()}'
        # Most pairs have no explicit range — that's fine, we accept any positive rate.
        # For configured pairs we enforce the range.
        if metric in validators.RANGES:
            if validators.in_range(metric, rate):
                out[ccy] = rate
        else:
            if rate > 0:
                out[ccy] = rate
    return out


def fetch() -> dict:
    s = http.get_session()

    # Daily snapshot
    r = s.get(URL_DAILY, timeout=20)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    day_nodes = root.findall('.//ecb:Cube/ecb:Cube', NS)
    if not day_nodes:
        raise ValueError('ECB daily XML had no <Cube time=...> node')
    today = _parse_day_node(day_nodes[0])
    today['rates'] = _validate_rates(today['rates'])
    if 'USD' not in today['rates']:
        raise ValueError('ECB daily missing USD rate after validation')

    # 90-day history
    r2 = s.get(URL_HIST_90D, timeout=25)
    r2.raise_for_status()
    root2 = ET.fromstring(r2.content)
    days = []
    for node in root2.findall('.//ecb:Cube/ecb:Cube', NS):
        parsed = _parse_day_node(node)
        parsed['rates'] = _validate_rates(parsed['rates'])
        if parsed['rates']:
            days.append(parsed)
    days.sort(key=lambda d: d['date'])

    # Build per-currency series for the priority list (frontend convenience)
    series: Dict[str, List[dict]] = {}
    for ccy in PRIORITY:
        pts = [{'date': d['date'], 'v': d['rates'][ccy]} for d in days if ccy in d['rates']]
        if pts:
            series[ccy] = pts

    # History: append today's rate for the major currencies
    rates_today = today.get('rates', {}) if isinstance(today, dict) else {}
    history.record_history('fx', {
        'usd': rates_today.get('USD'),
        'gbp': rates_today.get('GBP'),
        'chf': rates_today.get('CHF'),
        'jpy': rates_today.get('JPY'),
    })

    return {
        'data': {
            'current': today,
            'priority': PRIORITY,
            'series_90d': series,
            'all_rates_today': today['rates'],
        },
        'meta': {
            'source': 'European Central Bank',
            'license': 'public, no terms attached to reference rates',
            'units': 'EUR -> currency',
        },
    }
