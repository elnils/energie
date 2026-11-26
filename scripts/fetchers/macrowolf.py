"""
Makrowolf (macrowolf.eu) — gesamtwirtschaftliche Reihen mit Energiebezug.

Makrowolf legt jede Grafik als JSON unter https://macrowolf.eu/full/<name>.json
ab, Form: {"<reihen_id>": {"x": ["YYYY-MM-DD", ...], "y": [zahl, ...]}, ...}.
Hier werden nur die Reihen übernommen, die das Energie-Dashboard einordnen:
Großhandelspreis und Verbrauch Strom, Kraftstoff- und Heizölpreise,
Verbraucherpreise Energie, Erzeuger- und Importpreise, Endenergie nach Quelle
und Sektor, installierte Leistung, Preiserwartungen und Stimmung der Industrie.

Jede Datei ist einzeln abgesichert: fällt eine aus, bleiben die übrigen.
"""
from typing import Dict

from core import http

BASE = 'https://macrowolf.eu/full/'

# datei -> {reihen_id: (Bezeichnung, Einheit, Gruppe)}
SERIES = {
    'power': {
        'de_power_price':    ('Großhandelspreis Strom (Monatsmittel)', '€/MWh', 'strom'),
        'de_power_load_yoy': ('Stromverbrauch', '% zum Vorjahresmonat', 'strom'),
    },
    'fuel': {
        'de_fuel_e95':     ('Super E5 (Wochenmittel)', '€/Liter', 'kraftstoff'),
        'de_fuel_diesel':  ('Diesel (Wochenmittel)', '€/Liter', 'kraftstoff'),
        'de_fuel_heating': ('Heizöl (Wochenmittel)', '€/Liter', 'kraftstoff'),
    },
    'cpi': {
        'de_cpi_yoy':        ('Verbraucherpreise insgesamt', '% zum Vorjahr', 'preise'),
        'de_vpi_energy_yoy': ('Verbraucherpreise Energie', '% zum Vorjahr', 'preise'),
    },
    'pipeline_prices': {
        'de_ppi_yoy':           ('Erzeugerpreise gewerblicher Produkte', '% zum Vorjahr', 'preise'),
        'de_import_prices_yoy': ('Einfuhrpreise', '% zum Vorjahr', 'preise'),
    },
    'fec_quellen': {
        'de_fec_o4000xbio': ('Öl', 'TWh', 'endenergie_quelle'),
        'de_fec_g3000':     ('Erdgas', 'TWh', 'endenergie_quelle'),
        'de_fec_e7000':     ('Strom', 'TWh', 'endenergie_quelle'),
        'de_fec_h8000':     ('Fernwärme', 'TWh', 'endenergie_quelle'),
        'de_fec_ra000':     ('Erneuerbare direkt', 'TWh', 'endenergie_quelle'),
        'de_fec_kohle':     ('Kohle', 'TWh', 'endenergie_quelle'),
    },
    'fec_sektoren': {
        'de_fec_sek_fc_ind_e':    ('Industrie', 'TWh', 'endenergie_sektor'),
        'de_fec_sek_fc_tra_e':    ('Verkehr', 'TWh', 'endenergie_sektor'),
        'de_fec_sek_fc_oth_hh_e': ('Haushalte', 'TWh', 'endenergie_sektor'),
        'de_fec_sek_fc_oth_cp_e': ('Gewerbe, Handel, Dienstleistungen', 'TWh', 'endenergie_sektor'),
    },
    'cap_traeger': {
        'de_cap_ra420': ('Photovoltaik', 'GW', 'leistung'),
        'de_cap_ra300': ('Wind', 'GW', 'leistung'),
        'de_cap_cf':    ('Kohle, Gas, Öl, Biomasse', 'GW', 'leistung'),
        'de_cap_n9000': ('Kernkraft', 'GW', 'leistung'),
    },
    'price_exp': {
        'de_pexp_ind':  ('Preiserwartungen Industrie', 'Saldo', 'stimmung'),
        'de_pexp_cons': ('Preiserwartungen Verbraucher', 'Saldo', 'stimmung'),
    },
    'sentiment': {
        'de_conf_industry': ('Vertrauen Industrie', 'Saldo', 'stimmung'),
    },
}


def fetch() -> dict:
    s = http.get_session()
    out: Dict[str, dict] = {}
    errors = []
    for fname, wanted in SERIES.items():
        try:
            r = s.get(f'{BASE}{fname}.json', timeout=30)
            r.raise_for_status()
            payload = r.json()
        except Exception as e:
            errors.append(f'{fname}: {type(e).__name__}: {e}')
            print(f'  ! macrowolf/{fname}: {e}')
            continue
        for sid, (label, unit, group) in wanted.items():
            ser = payload.get(sid)
            if not isinstance(ser, dict):
                continue
            pts = [{'date': x, 'v': y} for x, y in zip(ser.get('x') or [], ser.get('y') or [])
                   if x and isinstance(y, (int, float))]
            if not pts:
                continue
            out[sid] = {'label': label, 'unit': unit, 'group': group,
                        'file': fname, 'series': pts}

    if not out:
        raise RuntimeError('Makrowolf: keine Reihe geladen; ' + '; '.join(errors))

    return {
        'data': {'series': out, 'errors': errors},
        'meta': {
            'source': 'Makrowolf (macrowolf.eu) – aufbereitet aus Destatis, SMARD/BNetzA, '
                      'EU-Kommission, Eurostat, AGEB',
            'url': 'https://macrowolf.eu',
            'api': BASE + '<datei>.json',
        },
    }
