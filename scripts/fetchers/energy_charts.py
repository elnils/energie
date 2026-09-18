"""
Energy-Charts API — verified against official OpenAPI v1.5 spec (Oct 2025).

Endpoints we use (all returning unix_seconds + production_types/price/...):
  /price                  — day-ahead spot per bidding zone (CC-BY 4.0 for many)
  /public_power           — net public generation by source
  /installed_power        — yearly installed capacity per source
  /ren_share_forecast     — RENEWABLE SHARE (correct name; /ren_share is deprecated)
  /cbpf                   — Cross-Border Physical Flows (in GW) — ENTSO-E proxy
  /frequency              — grid frequency at Freiburg

Endpoints we DO NOT call (per the official spec, they don't exist):
  /gas_price              — no such endpoint
  /co2_price              — no such endpoint
  → use commodities.* (Yahoo) for TTF gas and EU ETS proxies instead

Rate limits: v1.5 (Oct 2025) introduced stricter limits on the public API.
We sleep ~0.3s between calls and accept slower aggregate fetch time.
"""
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from core import http, history


BASE = 'https://api.energy-charts.info'


def _get(path: str, params: Dict) -> dict:
    s = http.get_session()
    r = s.get(f'{BASE}/{path}', params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def _first_list(d: dict, *keys: str) -> List:
    for k in keys:
        v = d.get(k)
        if isinstance(v, list) and v:
            return v
    return []


def fetch() -> dict:
    out: Dict[str, dict] = {}
    end = datetime.now(timezone.utc)
    end_str = end.strftime('%Y-%m-%d')
    start_7d   = (end - timedelta(days=7)).strftime('%Y-%m-%d')
    start_30d  = (end - timedelta(days=30)).strftime('%Y-%m-%d')

    # ── Day-ahead price per bidding zone ──
    # CC-BY 4.0 for DE-LU, AT, BE, CH, CZ, DK1, DK2, FR, HU, IT-North, NL, NO2, PL, SE4, SI
    for bzn, key in [
        ('DE-LU', 'price_de'), ('AT', 'price_at'), ('FR', 'price_fr'),
        ('PL', 'price_pl'),    ('CH', 'price_ch'), ('NL', 'price_nl'),
        ('BE', 'price_be'),    ('CZ', 'price_cz'), ('DK1', 'price_dk1'),
        ('DK2', 'price_dk2'),  ('NO2', 'price_no2'),('SI', 'price_si'),
        ('HU', 'price_hu'),    ('IT-North', 'price_it_north'),
    ]:
        try:
            d = _get('price', {'bzn': bzn, 'start': start_7d, 'end': end_str})
            out[key] = {
                'unix_seconds': d.get('unix_seconds', []),
                'price': d.get('price', []),
                'unit': d.get('unit', 'EUR/MWh'),
                'license': d.get('license_info', ''),
            }
            print(f'    ec/price {bzn}: {len(d.get("price", []))} pts')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! ec/price {bzn}: {str(e)[:80]}')
            out[key] = {'unix_seconds': [], 'price': [], 'unit': 'EUR/MWh'}

    # ── Long-term monthly DE-LU price ──
    try:
        new_ts = _get('price', {'bzn': 'DE-LU', 'start': '2018-10-01', 'end': end_str})
        # API doesn't natively support interval=month; we keep raw hourly and
        # the frontend can resample. To save space we instead grab the last 5 years.
        out['price_de_monthly'] = {
            'unix_seconds': new_ts.get('unix_seconds', []),
            'price': new_ts.get('price', []),
            'unit': 'EUR/MWh',
            'note': 'hourly resolution from 2018-10-01; frontend resamples to monthly avg',
        }
        time.sleep(0.3)
    except Exception as e:
        print(f'  ! ec/monthly: {str(e)[:80]}')
        out['price_de_monthly'] = {'unix_seconds': [], 'price': []}

    # ── Public power (generation mix) ──
    try:
        out['public_power_de'] = _get('public_power', {
            'country': 'de',
            'start': (end - timedelta(days=7)).strftime('%Y-%m-%dT%H:%MZ'),
            'end': end.strftime('%Y-%m-%dT%H:%MZ'),
        })
        time.sleep(0.3)
    except Exception as e:
        print(f'  ! ec/public_power: {str(e)[:80]}')

    # ── Renewable share forecast (NOT /ren_share — that's deprecated) ──
    try:
        d = _get('ren_share_forecast', {'country': 'de'})
        out['ren_share_de'] = {
            'unix_seconds': d.get('unix_seconds', []),
            'ren_share':         d.get('ren_share', []),
            'solar_share':       d.get('solar_share', []),
            'wind_onshore_share':  d.get('wind_onshore_share', []),
            'wind_offshore_share': d.get('wind_offshore_share', []),
            'is_forecast': True,
        }
        print(f'    ec/ren_share_forecast: {len(d.get("ren_share", []))} pts')
        time.sleep(0.3)
    except Exception as e:
        print(f'  ! ec/ren_share_forecast: {str(e)[:80]}')
        out['ren_share_de'] = {'unix_seconds': [], 'ren_share': []}

    # ── Installed power (yearly) ──
    try:
        out['installed_de'] = _get('installed_power',
                                   {'country': 'de', 'time_step': 'yearly'})
        time.sleep(0.3)
    except Exception as e:
        print(f'  ! ec/installed: {str(e)[:80]}')

    # ── Cross-border physical flows (ENTSO-E proxy, no key needed!) ──
    try:
        d = _get('cbpf', {'country': 'de',
                          'start': start_7d, 'end': end_str})
        out['cbpf_de'] = {
            'unix_seconds': d.get('unix_seconds', []),
            'countries': d.get('countries', []),
            'unit': 'GW',
            'note': 'positive = import to DE, negative = export from DE',
        }
        print(f'    ec/cbpf de: {len(d.get("countries", []))} neighbours')
        time.sleep(0.3)
    except Exception as e:
        print(f'  ! ec/cbpf: {str(e)[:80]}')
        out['cbpf_de'] = {'unix_seconds': [], 'countries': []}

    # ── Cross-border electricity trading (commercial flows) ──
    try:
        d = _get('cbet', {'country': 'de',
                          'start': start_7d, 'end': end_str})
        out['cbet_de'] = {
            'unix_seconds': d.get('unix_seconds', []),
            'countries': d.get('countries', []),
            'unit': 'GW',
        }
        time.sleep(0.3)
    except Exception as e:
        print(f'  ! ec/cbet: {str(e)[:80]}')

    # ── Grid frequency at Freiburg (1-second resolution, recent only) ──
    try:
        freq = _get('frequency', {
            'region': 'DE-Freiburg',
            'start': (end - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%MZ'),
            'end': end.strftime('%Y-%m-%dT%H:%MZ'),
        })
        out['frequency'] = {
            'unix_seconds': freq.get('unix_seconds', []),
            'data': freq.get('data', []),
            'unit': 'Hz',
        }
        time.sleep(0.3)
    except Exception as e:
        print(f'  ! ec/frequency: {str(e)[:80]}')

    # NOTE: No gas_price or co2_price — these endpoints don't exist in
    # the Energy-Charts API (verified against OpenAPI v1.5 spec). Use
    # commodities.ttf_eu_proxy and commodities.* instead in the frontend.
    out['gas_price'] = {'unix_seconds': [], 'price': [],
                       'note': 'See commodities.ttf_eu_proxy / commodities.natgas_henry'}
    out['co2_price'] = {'unix_seconds': [], 'price': [],
                       'note': 'No CO2 endpoint in Energy-Charts API'}

    _record_history(out)

    return {
        'data': out,
        'meta': {
            'source': 'Energy-Charts (Fraunhofer ISE)',
            'license': 'CC BY 4.0 for many bidding zones; see license_info per series',
            'api_version': 'v1.5',
            'endpoints_used': ['/price', '/public_power', '/installed_power',
                               '/ren_share_forecast', '/cbpf', '/cbet', '/frequency'],
        },
    }


def _record_history(out: Dict[str, dict]) -> None:
    """
    Archive today's day-ahead price per bidding zone to
    data/history/spot_price.jsonl.

    The API only serves a short rolling window — the dashboard's long-run
    view came from data/spot_history.json, a one-off snapshot that froze in
    April 2026 and has been drawn as if current ever since. Nothing was
    accumulating a fresh long-run record anywhere, so this closes the gap
    going forward: a few numbers per day per zone, which stays small in git
    while a decade of them is what makes the long view possible at all.
    """
    record: Dict[str, float] = {}
    for key, zone in [('price_de', 'de'), ('price_at', 'at'), ('price_fr', 'fr'),
                      ('price_pl', 'pl'), ('price_nl', 'nl'), ('price_be', 'be'),
                      ('price_ch', 'ch'), ('price_dk1', 'dk1')]:
        node = out.get(key) or {}
        seconds = node.get('unix_seconds') or []
        prices = node.get('price') or []
        if not seconds or not prices:
            continue
        # Today only, in UTC, matching how history keys its records.
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        todays = [p for t, p in zip(seconds, prices)
                  if p is not None
                  and datetime.fromtimestamp(t, timezone.utc).strftime('%Y-%m-%d') == today]
        if not todays:
            continue
        record[f'{zone}_avg'] = round(sum(todays) / len(todays), 2)
        record[f'{zone}_min'] = round(min(todays), 2)
        record[f'{zone}_max'] = round(max(todays), 2)
        if zone == 'de':
            record['de_hours'] = len(todays)
            # Hours at or below zero: the clearest single marker of a
            # renewables-heavy day, and not reconstructable later from an
            # average alone.
            record['de_negative_hours'] = sum(1 for p in todays if p <= 0)

    ren = out.get('ren_share_de') or {}
    shares = [v for v in (ren.get('ren_share') or []) if v is not None]
    if shares:
        record['de_ren_share_avg'] = round(sum(shares) / len(shares), 2)

    if record:
        history.record_history('spot_price', record)
