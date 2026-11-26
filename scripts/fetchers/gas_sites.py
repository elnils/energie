"""
Gasspeicher in Deutschland je Standort (GIE AGSI+).

1. Die Liste aller Betreiber und Speicher kommt ohne Schlüssel von
   https://agsi.gie.eu/api/about?show=listing.
2. Für jeden aktiven deutschen Speicher wird der letzte Gastag abgefragt
   (x-key = GIE_API_KEY): Füllstand %, Gas im Speicher (TWh), Arbeitsgas-
   volumen (TWh), Ein- und Ausspeicherung (GWh/d).
3. Mehrere Betreiber an einem Ort (Etzel, Epe, Jemgum, Nüttermoor) werden zu
   einem Standort zusammengefasst; Füllstand dort = Summe Gas / Summe Volumen.

Koordinaten: ungefähre Lage des Speicherorts (Ortslage), keine
Anlagengrenzen. Verbund-Angaben ohne eindeutigen Ort (etwa "EWE H-Gas Zone",
"VGS VTP Storage GPL") erscheinen in der Liste, aber nicht auf der Karte.
"""
import os
import time
from datetime import date
from typing import Dict, List, Optional

from core import http

LISTING = 'https://agsi.gie.eu/api/about?show=listing'
API = 'https://agsi.gie.eu/api'

# Schlüsselwort im Speichernamen -> (Standort, lat, lon)
ORTE = [
    ('Rehden',        'Rehden',            52.61,  8.48),
    # VSP NORD bündelt Rehden und Jemgum virtuell; nur Liste, sonst doppelt gezählt
    ('VSP NORD',      None, None, None),
    ('Jemgum',        'Jemgum',            53.27,  7.39),
    ('Etzel',         'Etzel',             53.47,  7.83),
    ('Epe',           'Epe (Gronau)',      52.18,  7.03),
    ('Nüttermoor',    'Nüttermoor',        53.25,  7.43),
    ('Rüdersdorf',    'Rüdersdorf',        52.47, 13.78),
    ('Kraak',         'Kraak',             53.45, 11.39),
    ('Frankenthal',   'Frankenthal',       49.53,  8.35),
    ('Ronnenberg',    'Empelde',           52.33,  9.66),
    ('Reckrod',       'Reckrod',           50.76,  9.74),
    ('Stockstadt',    'Stockstadt/Hähnlein', 49.80, 8.50),
    ('Inzenham',      'Inzenham-West',     47.84, 12.13),
    ('STASSFURT',     'Staßfurt',          51.86, 11.58),
    ('Uelsen',        'Uelsen',            52.50,  6.88),
    ('Harsefeld',     'Harsefeld',         53.45,  9.50),
    ('Peckensen',     'Peckensen',         52.74, 11.20),
    ('Lesum',         'Bremen-Lesum',      53.17,  8.70),
    ('Schmidhausen',  'Schmidhausen',      48.33, 11.92),
    ('Kiel',          'Kiel-Rönne',        54.29, 10.09),
    ('Allmenhausen',  'Allmenhausen/Kirchheilingen', 51.25, 10.68),
    ('Bierwang',      'Bierwang',          48.18, 12.21),
    ('Breitbrunn',    'Breitbrunn',        47.88, 12.37),
    ('Wolfersberg',   'Wolfersberg',       48.05, 11.83),
    ('Bernburg',      'Bernburg/Bad Lauchstädt/Katharina', 51.68, 11.78),
    ('Katharina',     'Bernburg/Bad Lauchstädt/Katharina', 51.68, 11.78),
    ('INNEXPOOL',     None, None, None),
]


def _ort(name: str):
    for key, ort, lat, lon in ORTE:
        if key.lower() in name.lower():
            return ort, lat, lon
    return None, None, None


def _num(v) -> Optional[float]:
    try:
        if v in (None, '', '-'):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _aktiv(f: dict, heute: str) -> bool:
    end = f.get('operational_end_date')
    return not end or end > heute


def fetch() -> dict:
    api_key = os.environ.get('GIE_API_KEY', '').strip()
    if len(api_key) < 10:
        raise RuntimeError('GIE_API_KEY missing or too short — register at agsi.gie.eu')
    s = http.get_session()
    r = s.get(LISTING, timeout=30)
    r.raise_for_status()
    heute = date.today().isoformat()
    anlagen = [(c, f) for c in r.json() if c.get('country') == 'DE'
               for f in c.get('facilities') or [] if _aktiv(f, heute)]

    speicher: List[dict] = []
    fehler = []
    for c, f in anlagen:
        try:
            rr = s.get(API, headers={'x-key': api_key}, timeout=25,
                       params={'country': 'DE', 'company': c['eic'], 'facility': f['eic'], 'size': 1})
            rr.raise_for_status()
            rows = (rr.json() or {}).get('data') or []
        except Exception as e:
            fehler.append(f"{f.get('name')}: {type(e).__name__}")
            rows = []
        time.sleep(0.4)
        if not rows:
            continue
        x = rows[0]
        ort, lat, lon = _ort(f.get('name', ''))
        speicher.append({
            'name': f.get('name'), 'betreiber': c.get('short_name') or c.get('name'),
            'eic': f.get('eic'), 'ort': ort, 'lat': lat, 'lon': lon,
            'datum': x.get('gasDayStart'),
            'fuellstand': _num(x.get('full')),
            'gas_twh': _num(x.get('gasInStorage')),
            'volumen_twh': _num(x.get('workingGasVolume')),
            'einspeisung_gwh': _num(x.get('injection')),
            'ausspeisung_gwh': _num(x.get('withdrawal')),
            'status': x.get('status'),
        })

    if not speicher:
        raise RuntimeError('AGSI: keine Speicherdaten je Anlage; ' + '; '.join(fehler[:5]))

    standorte: Dict[str, dict] = {}
    for sp in speicher:
        if not sp['ort']:
            continue
        o = standorte.setdefault(sp['ort'], {'ort': sp['ort'], 'lat': sp['lat'], 'lon': sp['lon'],
                                             'gas_twh': 0.0, 'volumen_twh': 0.0, 'netto_gwh': 0.0,
                                             'betreiber': [], 'speicher': 0, 'datum': sp['datum']})
        o['gas_twh'] += sp['gas_twh'] or 0
        o['volumen_twh'] += sp['volumen_twh'] or 0
        o['netto_gwh'] += (sp['einspeisung_gwh'] or 0) - (sp['ausspeisung_gwh'] or 0)
        o['speicher'] += 1
        if sp['betreiber'] not in o['betreiber']:
            o['betreiber'].append(sp['betreiber'])
        o['datum'] = max(o['datum'] or '', sp['datum'] or '')
    for o in standorte.values():
        o['fuellstand'] = round(100 * o['gas_twh'] / o['volumen_twh'], 2) if o['volumen_twh'] else None
        o['gas_twh'] = round(o['gas_twh'], 3)
        o['volumen_twh'] = round(o['volumen_twh'], 3)
        o['netto_gwh'] = round(o['netto_gwh'], 1)

    return {
        'data': {
            'standorte': sorted(standorte.values(), key=lambda o: -o['volumen_twh']),
            'speicher': sorted(speicher, key=lambda x: -(x['volumen_twh'] or 0)),
            'fehler': fehler,
        },
        'meta': {
            'source': 'GIE AGSI+ (Gas Infrastructure Europe)',
            'url': 'https://agsi.gie.eu',
            'note': 'Koordinaten = ungefähre Ortslage. Verbund-Angaben ohne eindeutigen Ort nur in der Liste.',
        },
    }
