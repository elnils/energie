"""
GIE AGSI+ — aggregated gas storage inventories.
GIE ALSI  — aggregated LNG terminal inventories.

Endpoints:
  https://agsi.gie.eu/api?country=<iso2>&size=<n>
  https://alsi.gie.eu/api?country=<iso2>&size=<n>

Auth: x-key header, register at https://agsi.gie.eu/account.
Rate limits: ~60 calls/min is safe.
Update times: 19:30 and 23:00 CET daily.

v5.3 fixes:
  - ALSI fill_pct was always None: GIE uses different field names per endpoint.
    Now tries a larger set of candidates and logs the first raw entry per
    source so future drift is visible without code archaeology.
  - AGSI country=EU returns total=0 since the v2 migration. We now synthesise
    the EU aggregate from the loaded country payloads (mirrors what the v4.3
    code did before the modular rewrite).
"""
import os
import time
from typing import Dict, List, Optional

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

# Field candidates for the percentage-fill indicator. AGSI canonical is 'full';
# ALSI uses different names depending on endpoint version. Listed in priority
# order — first non-None numeric in 0..100 wins.
FILL_FIELDS = (
    'full',                  # AGSI canonical, ALSI v2 sometimes
    'fullness',              # older ALSI naming
    'lngInventoryPercent',   # ALSI variant
    'inventoryFull',         # ALSI variant
    'gasInStoragePercent',   # AGSI variant
    'percentFull',           # generic
)

# Field candidates for the storage volume and capacity (ratio fallback).
STORED_FIELDS = ('gasInStorage', 'lngInventory', 'gasInStorageTWh')
CAPACITY_FIELDS = ('workingGasVolume', 'workingGasVolumeTWh',
                   'lngInventoryCapacity', 'dtmiInventoryCapacity')


def _to_float(v) -> Optional[float]:
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


def _first_float(entry: dict, fields: tuple) -> Optional[float]:
    """Return the first non-None numeric value among the listed fields."""
    for f in fields:
        v = _to_float(entry.get(f))
        if v is not None:
            return v
    return None


def _normalize_entry(entry: dict) -> Optional[dict]:
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

    # Fill % — try documented fields, then ratio fallback
    fill = _first_float(entry, FILL_FIELDS)
    if fill is None:
        stored = _first_float(entry, STORED_FIELDS)
        wgv = _first_float(entry, CAPACITY_FIELDS)
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
        'gas_in_storage_twh': _first_float(entry, STORED_FIELDS),
    }


def _fetch_one(api_url: str, api_key: str, country: str, size: int,
               diag_label: str = '') -> List[dict]:
    """
    AGSI/ALSI v2 API. Auth via x-key header. Returns cleaned rows.

    On first encountered raw entry per country, prints its keys to stdout.
    This makes field-name drift (the cause of the fill=None bug) instantly
    visible in the next run's log without needing source-code archaeology.
    """
    s = http.get_session()
    capped_size = min(size, 300)
    r = s.get(api_url, headers={'x-key': api_key},
              params={'country': country, 'size': capped_size}, timeout=25)
    if not r.ok:
        body = r.text[:200] if r.text else ''
        print(f'      {diag_label} {country} HTTP {r.status_code}: {body}')
        r.raise_for_status()
    payload = r.json()
    if isinstance(payload, dict):
        raw = payload.get('data') or payload.get('result') or payload.get('entries') or []
        if not raw and 'data' in payload:
            print(f'      {diag_label} {country}: empty data array '
                  f'(total={payload.get("total")}, last_page={payload.get("last_page")})')
    elif isinstance(payload, list):
        raw = payload
    else:
        raw = []

    # Diagnostic: dump the field names of the first row so any future drift
    # in GIE's field naming is obvious in the next run.
    if raw and isinstance(raw[0], dict):
        keys = sorted(raw[0].keys())
        print(f'      {diag_label} {country} fields[0]: {keys}')

    cleaned = []
    for entry in raw:
        norm = _normalize_entry(entry)
        if norm:
            cleaned.append(norm)
    cleaned.sort(key=lambda x: x['date'])
    return cleaned


def _synthesise_eu(country_data: Dict[str, dict]) -> List[dict]:
    """
    Build a daily EU aggregate from per-country payloads.
    For each date, fill_pct is the unweighted mean of per-country fills
    (matches how GIE used to report it before the v2 migration).
    Injection/withdrawal are summed across countries.
    """
    by_date: Dict[str, dict] = {}
    for code, node in country_data.items():
        if code == 'eu':
            continue
        for rec in node.get('data', []):
            d = rec.get('date')
            if not d:
                continue
            slot = by_date.setdefault(d, {'fills': [], 'inj': 0.0, 'with': 0.0})
            if rec.get('fill_pct') is not None:
                slot['fills'].append(rec['fill_pct'])
            slot['inj']  += rec.get('injection') or 0.0
            slot['with'] += rec.get('withdrawal') or 0.0
    out = []
    for d, slot in sorted(by_date.items()):
        out.append({
            'date': d,
            'fill_pct': round(sum(slot['fills']) / len(slot['fills']), 2) if slot['fills'] else None,
            'injection': round(slot['inj'], 4),
            'withdrawal': round(slot['with'], 4),
            'trend': None,
            'gas_in_storage_twh': None,
        })
    return out


def fetch() -> dict:
    api_key = os.environ.get('GIE_API_KEY', '').strip()
    if len(api_key) < 10:
        raise RuntimeError('GIE_API_KEY missing or too short — register at agsi.gie.eu')

    # ── AGSI (gas storage) ────────────────────────────────────────────
    gas: Dict[str, dict] = {}
    for code, name in COUNTRIES_GAS.items():
        try:
            data = _fetch_one('https://agsi.gie.eu/api', api_key,
                              code.upper(), size=400, diag_label='AGSI')
            gas[code] = {'name': name, 'data': data}
            last = data[-1] if data else None
            print(f'    agsi/{code}: {len(data)} pts, last={last["date"] if last else "—"} '
                  f'fill={last["fill_pct"] if last else "—"}%')
            time.sleep(0.2)
        except Exception as e:
            print(f'  ! agsi/{code}: {e}')
            gas[code] = {'name': name, 'data': []}

    # If AGSI EU is empty (the v2 API stopped returning it), synthesise it.
    if not gas.get('eu', {}).get('data'):
        synth = _synthesise_eu(gas)
        if synth:
            gas['eu'] = {'name': 'EU gesamt (computed)', 'data': synth}
            print(f'    agsi/eu (synth): {len(synth)} pts from country sums')

    # ── ALSI (LNG terminals) ──────────────────────────────────────────
    lng: Dict[str, dict] = {}
    for code, name in COUNTRIES_LNG.items():
        try:
            data = _fetch_one('https://alsi.gie.eu/api', api_key,
                              code.upper(), size=400, diag_label='ALSI')
            lng[code] = {'name': name, 'data': data}
            last = data[-1] if data else None
            print(f'    alsi/{code}: {len(data)} pts, last={last["date"] if last else "—"} '
                  f'fill={last["fill_pct"] if last else "—"}%')
            time.sleep(0.2)
        except Exception as e:
            print(f'  ! alsi/{code}: {e}')
            lng[code] = {'name': name, 'data': []}

    if not lng.get('eu', {}).get('data'):
        synth = _synthesise_eu(lng)
        if synth:
            lng['eu'] = {'name': 'EU gesamt (computed)', 'data': synth}
            print(f'    alsi/eu (synth): {len(synth)} pts from country sums')

    # ── History snapshot for trend chart ─────────────────────────────
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
            'note': 'EU aggregate synthesised when GIE returns empty country=EU',
        },
    }
