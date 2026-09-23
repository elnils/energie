"""
World Bank Indicators API — the G20 comparison Eurostat cannot provide.

Why this source
---------------
Every dataset in this dashboard that compares countries comes from Eurostat,
which serves EU members plus EFTA and candidates. The US, China, India,
Japan, Brazil and the rest of the G20 are not in those tables at any country
count, so "more countries" could never be answered by widening a GEO list.

The World Bank Indicators API covers every member, needs no API key, and
returns a stable two-element JSON envelope:

    [ {page, pages, per_page, total}, [ {countryiso3code, date, value}, ... ] ]

One request carries every country for one indicator, so the whole source is
a handful of calls.

Note this is a different service from the Pink Sheet spreadsheet that
energy_futures tries to discover and that keeps 404ing — that one is a
published document whose URL changes with each release; this is a versioned
REST API.

Indicator ids do get retired (the CO2 series moved from EN.ATM.CO2E.* to
EN.GHG.CO2.*.AR5 when the World Bank switched to AR5 warming potentials), so
each series carries a candidate list, the first that returns rows wins, and
the winner is recorded in meta.resolved.
"""
from typing import Dict, List, Optional, Tuple

from core import history, http

BASE = 'https://api.worldbank.org/v2'

# G20 members by ISO3, plus the EU aggregate the World Bank publishes.
# Names are the German ones the dashboard uses elsewhere.
G20 = {
    'ARG': 'Argentinien',   'AUS': 'Australien',  'BRA': 'Brasilien',
    'CAN': 'Kanada',        'CHN': 'China',       'DEU': 'Deutschland',
    'FRA': 'Frankreich',    'GBR': 'Vereinigtes Königreich',
    'IDN': 'Indonesien',    'IND': 'Indien',      'ITA': 'Italien',
    'JPN': 'Japan',         'KOR': 'Südkorea',    'MEX': 'Mexiko',
    'RUS': 'Russland',      'SAU': 'Saudi-Arabien', 'TUR': 'Türkei',
    'USA': 'USA',           'ZAF': 'Südafrika',   'EUU': 'Europäische Union',
}

START_YEAR = 1990

# key -> (candidate indicator ids, label, unit, higher_is_better)
# The flag is for the UI: a high renewable share is good news, a high
# emission total is not, and a chart that colours them alike misleads.
INDICATORS: Dict[str, Tuple[Tuple[str, ...], str, str, Optional[bool]]] = {
    'co2_total': (
        ('EN.GHG.CO2.MT.CE.AR5', 'EN.ATM.CO2E.KT'),
        'CO₂-Emissionen gesamt', 'Mio. t', False),
    'co2_per_capita': (
        ('EN.GHG.CO2.PC.CE.AR5', 'EN.ATM.CO2E.PC'),
        'CO₂ pro Kopf', 't pro Person', False),
    'renewable_share': (
        ('EG.FEC.RNEW.ZS',),
        'Erneuerbaren-Anteil am Endenergieverbrauch', '%', True),
    'renewable_electricity': (
        ('EG.ELC.RNEW.ZS',),
        'Erneuerbaren-Anteil an der Stromerzeugung', '%', True),
    'energy_use_per_capita': (
        ('EG.USE.PCAP.KG.OE',),
        'Energieverbrauch pro Kopf', 'kg Öläquivalent', None),
    'electricity_per_capita': (
        ('EG.USE.ELEC.KH.PC',),
        'Stromverbrauch pro Kopf', 'kWh', None),
    'fossil_share': (
        ('EG.USE.COMM.FO.ZS',),
        'Fossiler Anteil am Energieverbrauch', '%', False),
    'gdp_per_capita': (
        ('NY.GDP.PCAP.KD', 'NY.GDP.PCAP.CD'),
        'BIP pro Kopf', 'USD (konstant)', None),
    # Electricity mix and import dependency, for the G20 cards on the Strom,
    # Gas and Erneuerbare tabs. Some of these are archived by the World Bank
    # and stop in 2015; the chart names the year it shows.
    'elec_coal': (('EG.ELC.COAL.ZS',), 'Kohle an der Stromerzeugung', '%', False),
    'elec_gas': (('EG.ELC.NGAS.ZS',), 'Erdgas an der Stromerzeugung', '%', None),
    'elec_nuclear': (('EG.ELC.NUCL.ZS',), 'Kernkraft an der Stromerzeugung', '%', None),
    'elec_hydro': (('EG.ELC.HYRO.ZS',), 'Wasserkraft an der Stromerzeugung', '%', True),
    'elec_renew_ex_hydro': (('EG.ELC.RNWX.ZS',),
        'Wind, Solar & Co. an der Stromerzeugung', '%', True),
    'energy_imports': (('EG.IMP.CONS.ZS',),
        'Energieimporte netto (Anteil am Verbrauch)', '%', False),
    'co2_intensity_gdp': (('EN.GHG.CO2.RT.GDP.PP.KD', 'EN.ATM.CO2E.PP.GD.KD'),
        'CO₂ je BIP (Kaufkraft)', 'kg je USD', False),
}


def _fetch_indicator(indicator: str) -> Tuple[Dict[str, List[dict]], Optional[str]]:
    """
    One request for every country. Returns ({iso3: [{period, v}]}, error).

    An empty result is not an error the caller should retry: the World Bank
    answers a retired indicator with HTTP 200 and a message object instead of
    rows, which is what the candidate list exists for.
    """
    s = http.get_session()
    countries = ';'.join(G20)
    params = {
        'format': 'json',
        'per_page': 20000,
        'date': f'{START_YEAR}:2030',
    }
    try:
        r = s.get(f'{BASE}/country/{countries}/indicator/{indicator}',
                  params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()
    except Exception as e:
        return {}, f'{type(e).__name__}: {str(e)[:120]}'

    # The envelope is [meta, rows]; a retired or misspelled indicator returns
    # [{'message': [...]}] with no second element.
    if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
        msg = ''
        if isinstance(payload, list) and payload and isinstance(payload[0], dict):
            entries = payload[0].get('message') or []
            if entries and isinstance(entries[0], dict):
                msg = entries[0].get('value') or entries[0].get('key') or ''
        return {}, f'keine Datenzeilen{": " + msg[:100] if msg else ""}'

    out: Dict[str, List[dict]] = {}
    for row in payload[1]:
        if not isinstance(row, dict):
            continue
        iso = (row.get('countryiso3code') or '').strip().upper()
        year = str(row.get('date') or '').strip()
        value = row.get('value')
        if not iso or not year or value is None:
            continue
        try:
            out.setdefault(iso, []).append({'period': year, 'v': round(float(value), 3)})
        except (TypeError, ValueError):
            continue
    for iso in out:
        out[iso].sort(key=lambda p: p['period'])
    return out, None


def fetch() -> dict:
    data: Dict[str, dict] = {}
    resolved: Dict[str, str] = {}
    errors: List[str] = []

    for key, (candidates, label, unit, higher_better) in INDICATORS.items():
        series: Dict[str, List[dict]] = {}
        used: Optional[str] = None
        last_error: Optional[str] = None

        for indicator in candidates:
            series, last_error = _fetch_indicator(indicator)
            if series:
                used = indicator
                break
            print(f'    worldbank/{key} ({indicator}): {last_error or "leer"}'
                  + (', nächster Kandidat' if indicator != candidates[-1] else ''))

        total = sum(len(v) for v in series.values())
        if used:
            print(f'    worldbank/{key}: {len(series)} Länder, {total} Punkte [{used}]')
            resolved[key] = used
        else:
            msg = f'worldbank/{key}: kein Kandidat lieferte Daten ({last_error})'
            print(f'  ! {msg}')
            errors.append(msg)

        data[key] = {
            'series_per_country': series,
            'label': label,
            'unit': unit,
            'indicator': used,
            'higher_is_better': higher_better,
            'unavailable_reason': None if used else (last_error or 'unbekannt'),
        }

    if not any(v['series_per_country'] for v in data.values()):
        raise RuntimeError(
            'World Bank: kein Indikator lieferte Daten — '
            f'{"; ".join(errors) or "API nicht erreichbar"}'
        )

    data['countries'] = G20
    _record_history(data)

    empty = [k for k, v in data.items()
             if isinstance(v, dict) and 'series_per_country' in v
             and not v['series_per_country']]
    return {
        'data': data,
        'meta': {
            'source': 'World Bank Indicators API (api.worldbank.org/v2)',
            'license': 'CC BY 4.0',
            'note': ('Jahreswerte für alle G20-Mitglieder plus EU-Aggregat. '
                     'Die Serien haben unterschiedlich lange Verzüge: '
                     'Emissionen etwa zwei bis drei Jahre, Energieverbrauch '
                     'pro Kopf endet bei vielen Ländern 2014, weil die '
                     'Weltbank die Reihe nicht weitergeführt hat.'),
            'resolved': resolved,
            'empty_series': empty,
            'errors': errors,
        },
    }


def _record_history(data: Dict[str, dict]) -> None:
    """Latest German and Chinese figures, so a World Bank revision stays visible."""
    record: Dict[str, float] = {}
    for key in ('co2_total', 'co2_per_capita', 'renewable_share'):
        for iso in ('DEU', 'CHN', 'USA'):
            series = data.get(key, {}).get('series_per_country', {}).get(iso) or []
            if series:
                record[f'{iso.lower()}_{key}'] = series[-1]['v']
                record[f'{iso.lower()}_{key}_period'] = series[-1]['period']
    if record:
        history.record_history('worldbank_g20', record)
