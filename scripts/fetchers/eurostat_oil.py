"""
Eurostat Energy Statistics — umfassender Energie-Datenfetcher.

Dataset: nrg_cb_oilm (Oil & petroleum products monthly) — Haupt-Dataset
Zusätzlich: nrg_cb_gasm (Gas monthly), nrg_cb_e (Electricity), nrg_ind_ren (Renewables)

API: Eurostat Statistics API v1
Doku: https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access

URL-Struktur:
  https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}
  ?format=JSON&lang=EN&{dimension_filters}

Response-Format: JSON-stat 2.0
  - data.id[]    = Dimensionsliste
  - data.size[]  = Größe jeder Dimension
  - data.dimension.{dim}.category.index = {code: position}
  - data.value   = dict {str(pos): value} oder list

Fallback: Eurostat Bulk TSV Download (wenn REST leer antwortet)
  https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing
  ?file=data/{dataset}.tsv.gz

Warum 'Expecting value: line 1 column 1': r.json() auf leerem Body.
Fix: Content-Type + Body-Länge prüfen vor json().
"""
import gzip
import io
import os
import time
from typing import Dict, List, Optional, Tuple

from core import http

API_BASE  = 'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data'
BULK_BASE = 'https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing'

# Länder die wir für jede Serie holen
GEO_CODES = ['EU27_2020', 'DE', 'FR', 'IT', 'ES', 'NL', 'PL', 'BE', 'AT', 'CZ']

# ══════════════════════════════════════════════════════════════════════════════
# JSON-stat Parser
# ══════════════════════════════════════════════════════════════════════════════

def _parse_jsonstat(data: dict,
                   geo_dim: str = 'geo',
                   time_dim: str = 'time') -> Dict[str, List[dict]]:
    """
    Parst eine Eurostat JSON-stat 2.0 Antwort.

    Erwartet, dass alle Dimensionen außer geo und time auf genau
    eine Kategorie gefiltert wurden (per URL-Parameter). Das ermöglicht
    eine direkte Positions-Berechnung ohne Nested-Loop über alle Dimensionen.

    Returns: {geo_code: [{period: 'YYYY-MM', v: float}, ...]}
    """
    dims      = data.get('id', [])
    sizes     = data.get('size', [])
    dimension = data.get('dimension', {})
    values    = data.get('value', {})

    if not dims or not sizes or not values:
        return {}

    # Normalisiere values → {int_pos: float_or_none}
    if isinstance(values, dict):
        val_map = {int(k): v for k, v in values.items() if v is not None}
    else:
        val_map = {i: v for i, v in enumerate(values) if v is not None}

    # Stride (Schrittweite) für jede Dimension berechnen
    strides = [1] * len(dims)
    for i in range(len(dims) - 2, -1, -1):
        strides[i] = strides[i + 1] * sizes[i + 1]

    dim_pos   = {d: i for i, d in enumerate(dims)}
    # Eurostat uses 'TIME_PERIOD' in newer API, 'time' in older — try both
    for _td in (time_dim, 'TIME_PERIOD', 'time'):
        if _td in dimension:
            time_dim = _td
            break
    geo_cats  = dimension.get(geo_dim, {}).get('category', {}).get('index', {})
    time_cats = dimension.get(time_dim, {}).get('category', {}).get('index', {})

    if not geo_cats or not time_cats:
        return {}

    result: Dict[str, List[dict]] = {}

    for geo_code, geo_i in geo_cats.items():
        series = []
        for period, time_i in time_cats.items():
            # Lineare Position im Wert-Array
            pos = 0
            for d_idx, d_name in enumerate(dims):
                if d_name == geo_dim:
                    pos += geo_i * strides[d_idx]
                elif d_name == time_dim:
                    pos += time_i * strides[d_idx]
                # Alle anderen Dims: nur eine Kategorie → Index 0, Beitrag = 0

            val = val_map.get(pos)
            if val is not None:
                try:
                    series.append({'period': period, 'v': round(float(val), 2)})
                except (TypeError, ValueError):
                    pass

        if series:
            series.sort(key=lambda x: x['period'])
            result[geo_code] = series

    return result


# ══════════════════════════════════════════════════════════════════════════════
# HTTP-Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _safe_get(url: str, params: dict, timeout: int = 30) -> Optional[dict]:
    """
    GET mit Eurostat API. Gibt None zurück wenn:
    - Antwort nicht JSON (Content-Type check)
    - Body leer (verhindert 'Expecting value: line 1 column 1')
    - HTTP-Fehler
    - Timeout
    """
    s = http.get_session()
    try:
        r = s.get(url, params=params, timeout=timeout)
        if not r.ok:
            print(f'      eurostat HTTP {r.status_code}: {r.text[:80]}')
            return None
        ct = r.headers.get('content-type', '')
        if 'json' not in ct.lower():
            print(f'      eurostat non-JSON response ({ct[:40]}), skipping')
            return None
        body = r.text.strip()
        if not body or body.startswith('<') or body.startswith('Host not'):
            print(f'      eurostat empty/HTML body, skipping')
            return None
        return r.json()
    except Exception as e:
        print(f'      eurostat request error: {e}')
        return None


def _fetch_bulk_tsv(dataset: str) -> Optional[str]:
    """
    Fallback: Lade das komplette Dataset als TSV.gz vom Bulk-Download-Server.
    Eurostat Bulk-Download hat manchmal lockerer IP-Beschränkungen als REST API.
    Returns: TSV-Text oder None.
    """
    s = http.get_session()
    url = BULK_BASE
    params = {'file': f'data/{dataset}.tsv.gz', 'sort': '1'}
    try:
        r = s.get(url, params=params, timeout=60)
        if not r.ok or len(r.content) < 100:
            return None
        ct = r.headers.get('content-type', '')
        # ZIP/gzip magic bytes
        if r.content[:2] == b'\x1f\x8b':
            return gzip.decompress(r.content).decode('utf-8', errors='replace')
        elif r.content[:2] == b'PK':
            import zipfile
            with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                return zf.read(zf.namelist()[0]).decode('utf-8', errors='replace')
        elif b'\t' in r.content[:200]:
            # Plain TSV
            return r.content.decode('utf-8', errors='replace')
        return None
    except Exception as e:
        print(f'      eurostat bulk fallback {dataset}: {e}')
        return None


def _parse_bulk_tsv(tsv_text: str, siec: str, nrg_bal: str,
                    unit: str) -> Dict[str, List[dict]]:
    """
    Parst Eurostat Bulk-TSV für nrg_cb_oilm.
    Spaltenformat: freq,unit,siec,nrg_bal,geo\time  YYYY-MM  YYYY-MM ...
    Erste Zeile: Header mit Perioden als Spaltennamen
    Weitere Zeilen: Daten
    """
    lines = tsv_text.strip().split('\n')
    if len(lines) < 2:
        return {}

    header = lines[0].split('\t')
    # Erste Spalte enthält "freq,unit,siec,nrg_bal,geo" als zusammengeführten Key
    # Perioden ab Spalte 1
    periods = [h.strip() for h in header[1:]]

    result: Dict[str, List[dict]] = {}

    for line in lines[1:]:
        parts = line.split('\t')
        if not parts:
            continue
        key_str = parts[0]
        # Key enthält die Dimension-Werte kommagetrennt: M,KTOE,O4651,IC_OBS,DE
        key_parts = [k.strip() for k in key_str.split(',')]
        if len(key_parts) < 5:
            continue

        row_unit    = key_parts[1] if len(key_parts) > 1 else ''
        row_siec    = key_parts[2] if len(key_parts) > 2 else ''
        row_nrg_bal = key_parts[3] if len(key_parts) > 3 else ''
        row_geo     = key_parts[4] if len(key_parts) > 4 else ''

        if row_siec != siec or row_nrg_bal != nrg_bal or row_unit != unit:
            continue
        if row_geo not in GEO_CODES:
            continue

        series = []
        for i, period in enumerate(periods):
            val_raw = parts[i + 1].strip() if i + 1 < len(parts) else ''
            # Eurostat TSV kennzeichnet fehlende Werte mit ':' oder ': '
            val_clean = val_raw.split(' ')[0].strip()
            if val_clean in (':', '', 'b', 'p', 'e'):
                continue
            try:
                series.append({'period': period, 'v': round(float(val_clean), 2)})
            except ValueError:
                pass

        if series:
            series.sort(key=lambda x: x['period'])
            result[row_geo] = series

    return result


# ══════════════════════════════════════════════════════════════════════════════
# Serien-Definitionen
# ══════════════════════════════════════════════════════════════════════════════

# Format: (output_key, dataset, filter_params, description)
SERIES = [

    # ── nrg_cb_oilm — Öl & Mineralölprodukte monatlich ──────────────────────
    ('oil_jet_fuel_stocks',    'nrg_cb_oilm',
     {'unit': 'KTOE', 'siec': 'O4651', 'nrg_bal': 'INTSTOCK'},
     'Jet fuel inland stocks (KTOE)'),

    ('oil_jet_fuel_supply',    'nrg_cb_oilm',
     {'unit': 'KTOE', 'siec': 'O4651', 'nrg_bal': 'PPRD'},
     'Jet fuel primary production (KTOE)'),

    ('oil_jet_fuel_imports',   'nrg_cb_oilm',
     {'unit': 'KTOE', 'siec': 'O4651', 'nrg_bal': 'IMP'},
     'Jet fuel imports (KTOE)'),

    ('oil_jet_fuel_exports',   'nrg_cb_oilm',
     {'unit': 'KTOE', 'siec': 'O4651', 'nrg_bal': 'EXP'},
     'Jet fuel exports (KTOE)'),

    ('oil_jet_fuel_consumption','nrg_cb_oilm',
     {'unit': 'KTOE', 'siec': 'O4651', 'nrg_bal': 'FC_NE'},
     'Jet fuel final consumption (KTOE)'),

    ('oil_crude_imports',      'nrg_cb_oilm',
     {'unit': 'KTOE', 'siec': 'O4100_TOT', 'nrg_bal': 'IMP'},
     'Crude oil imports (KTOE)'),

    ('oil_crude_production',   'nrg_cb_oilm',
     {'unit': 'KTOE', 'siec': 'O4100_TOT', 'nrg_bal': 'PPRD'},
     'Crude oil domestic production (KTOE)'),

    ('oil_diesel_stocks',      'nrg_cb_oilm',
     {'unit': 'KTOE', 'siec': 'O4671XR5220B', 'nrg_bal': 'INTSTOCK'},
     'Gas/diesel oil stocks (KTOE)'),

    ('oil_motor_gasoline',     'nrg_cb_oilm',
     {'unit': 'KTOE', 'siec': 'O4652', 'nrg_bal': 'FC_NE'},
     'Motor gasoline final consumption (KTOE)'),

    ('oil_heating_oil_stocks', 'nrg_cb_oilm',
     {'unit': 'KTOE', 'siec': 'O46710', 'nrg_bal': 'INTSTOCK'},
     'Heating gas/diesel oil stocks (KTOE)'),

    # ── nrg_cb_gasm — Erdgas monatlich ───────────────────────────────────────
    ('gas_production',         'nrg_cb_gasm',
     {'unit': 'KTOE', 'siec': 'G3000', 'nrg_bal': 'PPRD'},
     'Natural gas production (KTOE)'),

    ('gas_imports',            'nrg_cb_gasm',
     {'unit': 'KTOE', 'siec': 'G3000', 'nrg_bal': 'IMP'},
     'Natural gas imports (KTOE)'),

    ('gas_exports',            'nrg_cb_gasm',
     {'unit': 'KTOE', 'siec': 'G3000', 'nrg_bal': 'EXP'},
     'Natural gas exports (KTOE)'),

    ('gas_consumption',        'nrg_cb_gasm',
     {'unit': 'KTOE', 'siec': 'G3000', 'nrg_bal': 'FC_NE'},
     'Natural gas final consumption (KTOE)'),

    ('gas_stocks',             'nrg_cb_gasm',
     {'unit': 'KTOE', 'siec': 'G3000', 'nrg_bal': 'INTSTOCK'},
     'Natural gas inland stocks (KTOE)'),

    # ── nrg_cb_e — Strom monatlich ────────────────────────────────────────────
    ('electricity_generation',  'nrg_cb_e',
     {'unit': 'GWH',  'siec': 'E7000', 'nrg_bal': 'GEP'},
     'Electricity gross production (GWh)'),

    ('electricity_imports',     'nrg_cb_e',
     {'unit': 'GWH',  'siec': 'E7000', 'nrg_bal': 'IMP'},
     'Electricity imports (GWh)'),

    ('electricity_exports',     'nrg_cb_e',
     {'unit': 'GWH',  'siec': 'E7000', 'nrg_bal': 'EXP'},
     'Electricity exports (GWh)'),

    ('electricity_consumption', 'nrg_cb_e',
     {'unit': 'GWH',  'siec': 'E7000', 'nrg_bal': 'FC_NE'},
     'Electricity final consumption (GWh)'),

    ('electricity_wind',        'nrg_cb_e',
     {'unit': 'GWH',  'siec': 'RA1000', 'nrg_bal': 'GEP'},
     'Wind electricity generation (GWh)'),

    ('electricity_solar',       'nrg_cb_e',
     {'unit': 'GWH',  'siec': 'RA300',  'nrg_bal': 'GEP'},
     'Solar PV electricity generation (GWh)'),

    ('electricity_nuclear',     'nrg_cb_e',
     {'unit': 'GWH',  'siec': 'N9000',  'nrg_bal': 'GEP'},
     'Nuclear electricity generation (GWh)'),

    ('electricity_hydro',       'nrg_cb_e',
     {'unit': 'GWH',  'siec': 'RA100',  'nrg_bal': 'GEP'},
     'Hydro electricity generation (GWh)'),
]

# Bulk-TSV Konfiguration für Fallback
BULK_CONFIGS = {
    'nrg_cb_oilm': ('KTOE', 'nrg_cb_oilm'),
    'nrg_cb_gasm': ('KTOE', 'nrg_cb_gasm'),
    'nrg_cb_e':    ('GWH',  'nrg_cb_e'),
}


def _fetch_one(output_key: str, dataset: str, filters: dict,
               bulk_cache: Dict[str, str]) -> Tuple[str, dict]:
    """
    Holt eine Serie via REST API, mit Bulk-TSV als Fallback.
    """
    # ── REST API Versuch ────────────────────────────────────────────────────
    params = {
        'format': 'JSON',
        'lang': 'EN',
        'freq': 'M',
        'sinceTimePeriod': '2018-01',
        **filters,
    }
    # Alle GEO auf einmal abrufen (durch kommagetrennte Werte in einem Parameter)
    # Eurostat erlaubt mehrere Werte: geo=EU27_2020&geo=DE&geo=FR etc.
    all_params = []
    for k, v in params.items():
        all_params.append((k, v))
    for geo in GEO_CODES:
        all_params.append(('geo', geo))

    s = http.get_session()
    try:
        r = s.get(f'{API_BASE}/{dataset}', params=all_params, timeout=45)
        if r.ok:
            ct = r.headers.get('content-type', '')
            body = r.text.strip()
            if 'json' in ct.lower() and body and not body.startswith('<'):
                data = r.json()
                per_country = _parse_jsonstat(data, geo_dim='geo', time_dim='time')
                if per_country:
                    total_pts = sum(len(v) for v in per_country.values())
                    print(f'    eurostat/{output_key}: {len(per_country)} countries, '
                          f'{total_pts} pts via REST')
                    return output_key, per_country
                else:
                    print(f'    eurostat/{output_key}: REST ok but 0 values parsed')
                _dims = data.get('id', [])
                _sizes = data.get('size', [])
                _dim_cats = {k: list(v.get('category',{}).get('index',{}).keys())[:2]
                              for k,v in data.get('dimension',{}).items()}
                print(f'      id={_dims} size={_sizes}')
                print(f'      cats={_dim_cats}')
            else:
                print(f'    eurostat/{output_key}: REST non-JSON ({ct[:30]}, '
                      f'body={body[:40]}), trying bulk...')
    except Exception as e:
        print(f'    eurostat/{output_key}: REST error ({e}), trying bulk...')

    time.sleep(0.3)

    # ── Bulk TSV Fallback ───────────────────────────────────────────────────
    if dataset not in bulk_cache:
        print(f'    eurostat: downloading bulk TSV for {dataset}...')
        tsv = _fetch_bulk_tsv(dataset)
        bulk_cache[dataset] = tsv or ''
        if tsv:
            print(f'    eurostat: bulk {dataset} ok ({len(tsv)//1024} KB)')

    tsv_text = bulk_cache.get(dataset, '')
    if tsv_text:
        siec    = filters.get('siec', '')
        nrg_bal = filters.get('nrg_bal', '')
        unit    = filters.get('unit', 'KTOE')
        per_country = _parse_bulk_tsv(tsv_text, siec, nrg_bal, unit)
        if per_country:
            total_pts = sum(len(v) for v in per_country.values())
            print(f'    eurostat/{output_key}: {len(per_country)} countries, '
                  f'{total_pts} pts via bulk TSV')
            return output_key, per_country

    print(f'  ! eurostat/{output_key}: both REST and bulk failed')
    return output_key, {}


def fetch() -> dict:
    """
    Holt alle Energie-Serien von Eurostat.
    REST API → Bulk TSV Fallback → leere Serie (stale=true).
    Wirft RuntimeError nur wenn ALLE Serien leer sind.
    """
    results = {}
    any_success = False
    bulk_cache: Dict[str, str] = {}   # dataset → TSV-Text (einmal pro Dataset laden)

    for output_key, dataset, filters, desc in SERIES:
        try:
            _, per_country = _fetch_one(output_key, dataset, filters, bulk_cache)
            if per_country:
                any_success = True
            results[output_key] = {
                'series_per_country': per_country,
                'description': desc,
                'dataset': dataset,
                'filters': filters,
                'unit': filters.get('unit', 'KTOE'),
            }
            time.sleep(0.2)
        except Exception as e:
            print(f'  ! eurostat {output_key}: {e}')
            results[output_key] = {
                'series_per_country': {},
                'description': desc,
                'dataset': dataset,
            }

    # Kompatibilitäts-Alias für Frontend (erwartet jet_stocks, jet_imports, ...)
    aliases = {
        'jet_stocks':      'oil_jet_fuel_stocks',
        'jet_production':  'oil_jet_fuel_supply',
        'jet_imports':     'oil_jet_fuel_imports',
        'jet_consumption': 'oil_jet_fuel_consumption',
    }
    for alias, source in aliases.items():
        if alias not in results and source in results:
            results[alias] = results[source]

    if not any_success:
        raise RuntimeError(
            'Eurostat: alle Serien fehlgeschlagen — API nicht erreichbar '
            'oder alle Produkt-/Flow-Kombinationen leer. '
            'Letzter Datensatz wird als stale beibehalten.'
        )

    return {
        'data': results,
        'meta': {
            'source': 'Eurostat — Energy statistics (Eurostat Statistics API)',
            'datasets': list({s[1] for s in SERIES}),
            'license': 'Eurostat open data (free reuse)',
            'url': 'https://ec.europa.eu/eurostat/web/energy/overview',
            'units': 'KTOE (Kilotonnes of oil equivalent) for oil/gas; GWh for electricity',
            'note': (
                f'{len(SERIES)} Serien aus 3 Datasets: '
                'nrg_cb_oilm (Öl, 10 Serien), '
                'nrg_cb_gasm (Erdgas, 5 Serien), '
                'nrg_cb_e (Strom, 8 Serien). '
                'REST API mit Bulk-TSV Fallback.'
            ),
        },
    }
