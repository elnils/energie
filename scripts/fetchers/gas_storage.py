"""
GIE AGSI+ — aggregated gas storage inventories.
GIE ALSI    — aggregated LNG terminal inventories.

Bug-fix vs v4.3: the previous implementation queried the API with no auth
header, which returns either an empty list or a generic error. AGSI/ALSI
require the `x-key` header containing the registered API key.

Endpoints:
  https://agsi.gie.eu/api?country=<iso2>&size=<n>
  https://alsi.gie.eu/api?country=<iso2>&size=<n>
  Aggregated EU: country=eu

Rate limits: not officially documented but ~60 calls/min is safe.
Update times: 19:30 and 23:00 CET daily.
"""
import os
import time
from typing import Dict, List

from core import http, history


COUNTRIES_GAS = {
    'eu': 'EU gesamt',
    'de': 'Deutschland',
    'at': 'Österreich',
    'fr': 'Frankreich',
    'it': 'Italien',
    'nl': 'Niederlande',
    'be': 'Belgien',
    'pl': 'Polen',
    'es': 'Spanien',
    'cz': 'Tschechien',
    'sk': 'Slowakei',
    'hu': 'Ungarn',
    'ua': 'Ukraine',
}

COUNTRIES_LNG = {
    'eu': 'EU gesamt',
    'es': 'Spanien',
    'fr': 'Frankreich',
    'it': 'Italien',
    'nl': 'Niederlande',
    'be': 'Belgien',
    'de': 'Deutschland',
    'pl': 'Polen',
    'pt': 'Portugal',
    'gr': 'Griechenland',
    'hr': 'Kroatien',
    'lt': 'Litauen',
}


def _to_float(v) -> float | None:
    """Best-effort float parse (handles German decimals, empty strings)."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(',', '.')
    if not s or s.lower() in ('nan', 'null', 'none', '-'):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _normalize_entry(entry: dict) -> dict | None:
    """Pick consistent fields out of one AGSI/ALSI row."""
    if not isinstance(entry, dict):
        return None
    date = ''
    for k in ('gasDayStart', 'gasDayStartedOn', 'date', 'reportingPeriod'):
        v = str(entry.get(k) or '')
        if len(v) >= 10:
            date = v[:10]
            break
    if not date:
        return None

    # Fill % — try documented field first, then fallbacks
    fill = _to_float(entry.get('full'))
    if fill is None:
        # Some endpoints use other field names; ratio fallback as last resort
        stored = _to_float(entry.get('gasInStorage'))
        wgv = _to_float(entry.get('workingGasVolume'))
        if stored is not None and wgv and wgv > 0:
            fill = round(stored / wgv * 100.0, 2)
    if fill is not None and not (0 <= fill <= 100):
        fill = None

    return {
        'date': date,
        'fill_pct': fill,
        'injection': _to_float(entry.get('injection')) or 0.0,
        'withdrawal': _to_float(entry.get('withdrawal')) or 0.0,
        'trend': _to_float(entry.get('trend')),
        'gas_in_storage_twh': _to_float(entry.get('gasInStorage')),
    }


def _fetch_one(api_url: str, api_key: str, country: str, size: int) -> List[dict]:
    """
    AGSI/ALSI v2 API: paginated, max size=300 per request.
    Auth via x-key header. Returns the raw `data` list from the response.
    """
    s = http.get_session()
    # Cap size at 300 — API rejects larger
    capped_size = min(size, 300)
    r = s.get(api_url, headers={'x-key': api_key},
              params={'country': country, 'size': capped_size}, timeout=25)
    if not r.ok:
        # Show the response body for diagnostics — AGSI typically returns
        # a JSON {error: "..."} on auth/parameter problems
        body = r.text[:200] if r.text else ''
        print(f'      AGSI {country} HTTP {r.status_code}: {body}')
        r.raise_for_status()
    payload = r.json()
    if isinstance(payload, dict):
        raw = payload.get('data') or payload.get('result') or payload.get('entries') or []
        # Pagination info comes in last_page / total — if first page, that's enough
        if not raw and 'data' in payload:
            print(f'      AGSI {country}: empty data array (response: {str(payload)[:120]})')
    elif isinstance(payload, list):
        raw = payload
    else:
        raw = []
    cleaned = []
    for entry in raw:
        norm = _normalize_entry(entry)
        if norm:
            cleaned.append(norm)
    cleaned.sort(key=lambda x: x['date'])
    return cleaned


def fetch() -> dict:
    api_key = os.environ.get('GIE_API_KEY', '').strip()
    if len(api_key) < 10:
        raise RuntimeError('GIE_API_KEY missing or too short — register at agsi.gie.eu')

    gas: Dict[str, dict] = {}
    for code, name in COUNTRIES_GAS.items():
        try:
            data = _fetch_one('https://agsi.gie.eu/api', api_key, code.upper(), size=400)
            gas[code] = {'name': name, 'data': data}
            last = data[-1] if data else None
            print(f'    agsi/{code}: {len(data)} pts, last={last["date"] if last else "—"} '
                  f'fill={last["fill_pct"] if last else "—"}%')
            time.sleep(0.2)
        except Exception as e:
            print(f'  ! agsi/{code}: {e}')
            gas[code] = {'name': name, 'data': []}

    lng: Dict[str, dict] = {}
    for code, name in COUNTRIES_LNG.items():
        try:
            data = _fetch_one('https://alsi.gie.eu/api', api_key, code.upper(), size=400)
            lng[code] = {'name': name, 'data': data}
            last = data[-1] if data else None
            print(f'    alsi/{code}: {len(data)} pts, last={last["date"] if last else "—"} '
                  f'fill={last["fill_pct"] if last else "—"}%')
            time.sleep(0.2)
        except Exception as e:
            print(f'  ! alsi/{code}: {e}')
            lng[code] = {'name': name, 'data': []}

    # History: snapshot per-country fill levels for trend analysis
    hist_record = {}
    for code in ('eu', 'de', 'at', 'fr', 'it', 'nl', 'pl'):
        node = gas.get(code, {})
        last = next((x for x in reversed(node.get('data', [])) if x.get('fill_pct') is not None), None)
        if last:
            hist_record[f'{code}_pct'] = last['fill_pct']
    history.record_history('gas_storage', hist_record)

    return {
        'data': {
            'gas': gas,
            'lng': lng,
        },
        'meta': {
            'source': 'GIE AGSI+ / ALSI',
            'license': 'free for non-commercial use, attribution requested',
            'units': 'fill_pct = percent of working gas volume; injection/withdrawal in TWh/day',
        },
    }
