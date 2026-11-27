"""
Energiewende vor Ort: Kennzahlen je Kreis (400 Kreise und kreisfreie Städte).

  Kreise, Einwohner, Fläche       Statistisches Bundesamt, Gemeindeverzeichnis
                                  (04-kreise.xlsx, mit NUTS-3-Code je Kreis)
  Solaranlagen, Batteriespeicher  Marktstammdatenregister der Bundesnetzagentur:
                                  Anzahl Einheiten in Betrieb je Kreis, gezählt
                                  über den Gemeindeschlüssel (ein Abruf je Kreis
                                  und Träger, nur die Trefferzahl)
  Windenergie an Land und See     Marktstammdatenregister: alle Anlagen in
                                  Betrieb (rund 32 000), Anzahl und Leistung je
                                  Kreis; Anlagen auf See ohne Kreis zählen nur
                                  bundesweit
  Ladepunkte                      Ladesäulenregister der Bundesnetzagentur
  Elektroautos                    Kraftfahrt-Bundesamt, FZ1 (Pkw-Bestand am
                                  1. Januar nach Zulassungsbezirken und
                                  Kraftstoffarten)

Takt: täglicher Aufruf. Die Zählung im Marktstammdatenregister ist langsam
(rund zwei Sekunden je Abruf, 800 Abrufe), deshalb zählt jeder Lauf einige
Minuten lang die Kreise mit den ältesten Werten nach; nach wenigen Tagen ist
einmal ganz Deutschland erneuert. Wind, Ladesäulen und KBA werden höchstens
einmal pro Woche neu geladen.

Ausgabe: data.kreise[<NUTS3>] = {ags, name, art, land, ew, km2, solar_n,
speicher_n, wind_n, wind_mw, lade_lp, lade_kw, pkw, bev, phev} sowie data.stand
und data.quellen. Jede Quelle ist einzeln abgesichert; fällt eine aus oder
wird ein Teil nicht fertig, bleiben die Werte des letzten Laufs stehen.
"""
import csv
import io
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone

from core import http, paths

KREISE_XLSX = ('https://www.destatis.de/DE/Themen/Laender-Regionen/Regionales/'
               'Gemeindeverzeichnis/Administrativ/04-kreise.xlsx?__blob=publicationFile')
MASTR = ('https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/'
         'GetErweiterteOeffentlicheEinheitStromerzeugung')
LADE_SEITE = 'https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html'
KBA = 'https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ1/fz1_{jahr}.xlsx?__blob=publicationFile'

# KBA weist manche Stadt und den umgebenden Kreis gemeinsam aus; der Wert wird
# nach Einwohnern aufgeteilt (Anteile bleiben dabei für beide gleich)
KBA_GEMEINSAM = {'07211': ('07211', '07235')}     # Trier und Trier-Saarburg

TRAEGER = {'solar': '2495', 'speicher': '2496', 'wind': '2497'}
IN_BETRIEB = '35'
ZAEHL_FRIST = int(os.environ.get('VOR_ORT_FRIST', '420'))   # Sekunden für Zählabrufe je Lauf
WOCHE = 6                  # Tage, bis Wind, Ladesäulen und KBA neu geladen werden

# Ladesäulenregister: Namen, die sich nicht aus dem Kreisnamen ergeben
LADE_ALIAS = {
    'kreisfreie stadt hanau': '06435',              # seit 2026 kreisfrei, in der Statistik noch Main-Kinzig-Kreis
    'kreisfreie stadt oldenburg (oldb)': '03403',
}


def _vorher() -> dict:
    try:
        with open(os.path.join(paths.DATA_DIR, 'vor_ort.json'), encoding='utf-8') as fh:
            raw = json.load(fh)
        return raw.get('data') or raw
    except Exception:
        return {}


def _frisch(stand: dict, teil: str) -> bool:
    """True, wenn dieser Teil vor weniger als WOCHE Tagen geladen wurde."""
    d = (stand or {}).get('geladen', {}).get(teil)
    try:
        return (date.today() - date.fromisoformat(d)).days < WOCHE
    except Exception:
        return False


def _norm(name: str) -> str:
    s = name.lower().replace('ß', 'ss').split(',')[0]
    while True:
        t = re.sub(r'^(landkreis|kreisfreie stadt|kreis|stadtkreis|städteregion|regionalverband|region)\s+', '', s)
        if t == s:
            break
        s = t
    return re.sub(r'[^a-zäöü]+', '', s)


# ── Kreise ──────────────────────────────────────────────────────────────────
def _kreise(s) -> dict:
    import openpyxl
    r = s.get(KREISE_XLSX, timeout=60)
    r.raise_for_status()
    wb = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True)
    ws = next(wb[n] for n in wb.sheetnames if 'kreis' in n.lower() and 'deck' not in n.lower())
    land, out, titel = None, {}, ''
    for row in ws.iter_rows(values_only=True):
        if not titel and row and row[0]:
            titel = str(row[0])
        ags = str(row[0] or '').strip()
        if re.fullmatch(r'\d{2}', ags) and row[1]:
            land = str(row[1]).strip()
        if re.fullmatch(r'\d{5}', ags) and row[3] and row[5]:
            out[str(row[3]).strip()] = {
                'ags': ags, 'name': str(row[2]).strip(), 'art': str(row[1]).strip(),
                'land': land, 'km2': round(float(row[4]), 1), 'ew': int(row[5]),
            }
    if len(out) < 390:
        raise RuntimeError(f'Destatis: nur {len(out)} Kreise gelesen')
    m = re.search(r'am (\d{2}\.\d{2}\.\d{4})', titel)
    return out, (m.group(1) if m else None)


# ── Marktstammdatenregister ─────────────────────────────────────────────────
def _mastr(s, filt: str, page=1, size=1) -> dict:
    r = s.get(MASTR, timeout=90, params={'sort': '', 'page': page, 'pageSize': size, 'group': '', 'filter': filt})
    r.raise_for_status()
    d = r.json()
    if d.get('Errors'):
        raise RuntimeError(str(d['Errors'])[:200])
    return d


def _zaehlen(s, kreise: dict, vorher: dict) -> tuple:
    """Anzahl Solaranlagen und Speicher in Betrieb je Kreis (nur Trefferzahl).
    Reihenfolge: fehlende Werte zuerst, dann die ältesten; was in der Frist
    nicht dran war, behält den Wert und das Datum des letzten Laufs."""
    def alter(nuts, art):
        v = vorher.get(nuts) or {}
        return ('0' if v.get(f'{art}_n') is None else v.get(f'z_{art}') or '1')
    auftraege = sorted(((nuts, k['ags'], art) for nuts, k in kreise.items() for art in ('solar', 'speicher')),
                       key=lambda a: alter(a[0], a[2]))
    ergebnis, fehler, ende = {}, 0, time.time() + ZAEHL_FRIST
    heute = date.today().isoformat()

    def eins(ags, art):
        if time.time() > ende:
            return None
        f = (f"Gemeindeschlüssel~sw~'{ags}'~and~Energieträger~eq~'{TRAEGER[art]}'"
             f"~and~Betriebs-Status~eq~'{IN_BETRIEB}'")
        return int(_mastr(s, f)['Total'])

    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(eins, ags, art): (nuts, art) for nuts, ags, art in auftraege}
        for f in as_completed(futs):
            nuts, art = futs[f]
            try:
                v = f.result()
            except Exception:
                v, fehler = None, fehler + 1
            if v is None:
                ergebnis[(nuts, art)] = ((vorher.get(nuts) or {}).get(f'{art}_n'), (vorher.get(nuts) or {}).get(f'z_{art}'))
            else:
                ergebnis[(nuts, art)] = (v, heute)
    return ergebnis, fehler


def _wind(s) -> tuple:
    """Alle Windanlagen in Betrieb; Anzahl und Leistung (MW) je Kreisschlüssel."""
    f = f"Energieträger~eq~'{TRAEGER['wind']}'~and~Betriebs-Status~eq~'{IN_BETRIEB}'"
    je, see_n, see_mw, gesamt, seite = {}, 0, 0.0, None, 1
    while True:
        d = _mastr(s, f, page=seite, size=5000)
        gesamt = d.get('Total', 0)
        daten = d.get('Data') or []
        for e in daten:
            kw = float(e.get('Bruttoleistung') or 0)
            gs = str(e.get('Gemeindeschluessel') or '')
            if re.fullmatch(r'\d{8}', gs):
                a = je.setdefault(gs[:5], [0, 0.0])
                a[0] += 1
                a[1] += kw / 1000
            else:
                see_n += 1
                see_mw += kw / 1000
        if not daten or seite * 5000 >= gesamt:
            break
        seite += 1
    gezaehlt = sum(a[0] for a in je.values()) + see_n
    if gesamt and gezaehlt < gesamt * 0.95:
        raise RuntimeError(f'Wind: nur {gezaehlt} von {gesamt} Anlagen gelesen')
    return je, {'n': see_n, 'mw': round(see_mw, 1)}


# ── Ladesäulenregister ──────────────────────────────────────────────────────
def _lade(s, kreise: dict) -> tuple:
    seite = s.get(LADE_SEITE, timeout=40).text
    links = re.findall(r'https://data\.bundesnetzagentur\.de/[^"\']*Ladesaeulenregister[^"\']*\.csv', seite)
    if not links:
        raise RuntimeError('Ladesäulenregister: kein CSV-Link gefunden')
    url = sorted(set(links))[-1]
    r = s.get(url, timeout=180)
    r.raise_for_status()
    try:
        txt = r.content.decode('utf-8')
    except UnicodeDecodeError:
        txt = r.content.decode('cp1252')
    zeilen = txt.splitlines()
    start = next(i for i, z in enumerate(zeilen) if z.startswith('Ladeeinrichtungs-ID'))
    rd = csv.reader(zeilen[start:], delimiter=';')
    kopf = next(rd)
    ik, ist = kopf.index('Kreis/kreisfreie Stadt'), kopf.index('Status')
    ilp, ikw = kopf.index('Anzahl Ladepunkte'), kopf.index('Nennleistung Ladeeinrichtung [kW]')

    nach_name = {}
    for nuts, k in kreise.items():
        stadt = k['art'] in ('Kreisfreie Stadt', 'Stadtkreis')
        nach_name[(stadt, _norm(k['name']))] = k['ags']
    je, ohne = {}, 0
    for z in rd:
        if len(z) <= max(ik, ilp, ikw) or z[ist] != 'In Betrieb':
            continue
        roh = z[ik].strip()
        ags = LADE_ALIAS.get(roh.lower())
        if not ags:
            stadt = roh.startswith(('Kreisfreie Stadt', 'Stadtkreis'))
            ags = nach_name.get((stadt, _norm(roh))) or nach_name.get((not stadt, _norm(roh)))
        if not ags:
            ohne += 1
            continue
        a = je.setdefault(ags, [0, 0.0])
        try:
            a[0] += int(z[ilp] or 0)
            a[1] += float((z[ikw] or '0').replace(',', '.'))
        except ValueError:
            pass
    m = re.search(r'(\d{4}-\d{2}-\d{2})', url)
    return je, ohne, (m.group(1) if m else None)


# ── Kraftfahrt-Bundesamt ────────────────────────────────────────────────────
def _kba(s) -> tuple:
    import openpyxl
    for jahr in (date.today().year, date.today().year - 1):
        r = s.get(KBA.format(jahr=jahr), timeout=60)
        if r.status_code != 200 or not r.content.startswith(b'PK'):
            continue
        wb = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True)
        ws = next((wb[n] for n in wb.sheetnames if n.replace(' ', '') == 'FZ1.2'), None)
        if ws is None:
            continue
        je = {}
        for row in ws.iter_rows(values_only=True):
            for i, c in enumerate(row[:4]):
                m = re.match(r'^(\d{5})\s', str(c or ''))
                if m:
                    w = row[i + 1:i + 8]   # insgesamt, Benzin, Diesel, Gas, Hybrid, Plug-in, Elektro
                    if all(isinstance(x, (int, float)) for x in (w[0], w[5], w[6])):
                        a = je.setdefault(m.group(1), [0, 0, 0])
                        a[0] += int(w[0]); a[1] += int(w[6]); a[2] += int(w[5])
                    break
        if len(je) > 350:
            return je, f'{jahr}-01-01'
    raise RuntimeError('KBA FZ1: keine Tabelle FZ1.2 gefunden')


def fetch() -> dict:
    s = http.get_session()
    alt_daten = _vorher()
    vorher = alt_daten.get('kreise') or {}
    alt_stand = alt_daten.get('stand') or {}
    kreise, kreise_stand = _kreise(s)
    stand = {'kreise': kreise_stand, 'geladen': dict(alt_stand.get('geladen') or {})}
    for t in ('wind', 'lade', 'kba', 'mastr'):
        if alt_stand.get(t):
            stand[t] = alt_stand[t]
    fehler, heute = [], date.today().isoformat()

    def uebernehmen(*felder):
        for nuts, k in kreise.items():
            for f in felder:
                k[f] = (vorher.get(nuts) or {}).get(f)

    # Windenergie (vollständig, mit Leistung) – höchstens einmal pro Woche
    see = alt_daten.get('wind_see')
    if _frisch(alt_stand, 'wind') and vorher:
        uebernehmen('wind_n', 'wind_mw')
    else:
        try:
            wind, see = _wind(s)
            for nuts, k in kreise.items():
                n, mw = wind.get(k['ags'], [0, 0.0])
                k['wind_n'], k['wind_mw'] = n, round(mw, 1)
            stand['wind'] = stand['geladen']['wind'] = heute
        except Exception as e:
            fehler.append(f'Wind: {type(e).__name__}: {e}'[:200])
            uebernehmen('wind_n', 'wind_mw')

    # Ladepunkte – höchstens einmal pro Woche
    if _frisch(alt_stand, 'lade') and vorher:
        uebernehmen('lade_lp', 'lade_kw')
    else:
        try:
            lade, ohne, lade_stand = _lade(s, kreise)
            for nuts, k in kreise.items():
                lp, kw = lade.get(k['ags'], [0, 0.0])
                k['lade_lp'], k['lade_kw'] = lp, round(kw)
            stand['lade'], stand['geladen']['lade'] = lade_stand, heute
            if ohne:
                fehler.append(f'Ladesäulen: {ohne} Einrichtungen ohne Kreiszuordnung')
        except Exception as e:
            fehler.append(f'Ladesäulen: {type(e).__name__}: {e}'[:200])
            uebernehmen('lade_lp', 'lade_kw')

    # Elektroautos – höchstens einmal pro Woche (erscheint jährlich)
    if _frisch(alt_stand, 'kba') and vorher:
        uebernehmen('pkw', 'bev', 'phev', 'kba_geteilt')
    else:
        try:
            kba, kba_stand = _kba(s)
            nach_ags = {k['ags']: k for k in kreise.values()}
            for code, teile in KBA_GEMEINSAM.items():
                if code in kba and all(t in nach_ags for t in teile) and not any(t in kba for t in teile if t != code):
                    ges, ew = kba.pop(code), sum(nach_ags[t]['ew'] for t in teile)
                    for t in teile:
                        kba[t] = [round(x * nach_ags[t]['ew'] / ew) for x in ges]
                        nach_ags[t]['kba_geteilt'] = True
            for nuts, k in kreise.items():
                k['pkw'], k['bev'], k['phev'] = kba.get(k['ags'], [None, None, None])
            stand['kba'], stand['geladen']['kba'] = kba_stand, heute
        except Exception as e:
            fehler.append(f'KBA: {type(e).__name__}: {e}'[:200])
            uebernehmen('pkw', 'bev', 'phev', 'kba_geteilt')

    # Solar und Speicher: rollierende Zählung je Kreis
    try:
        z, nfehl = _zaehlen(s, kreise, vorher)
        for nuts, k in kreise.items():
            for art in ('solar', 'speicher'):
                k[f'{art}_n'], k[f'z_{art}'] = z.get((nuts, art), (None, None))
        daten = [k[f'z_{a}'] for k in kreise.values() for a in ('solar', 'speicher') if k.get(f'z_{a}')]
        if daten:
            stand['mastr'] = min(daten)          # ältester Zählstand
            stand['mastr_bis'] = max(daten)
        if nfehl:
            fehler.append(f'MaStR: {nfehl} Zählabrufe ohne Antwort (Werte vom letzten Lauf)')
    except Exception as e:
        fehler.append(f'MaStR: {type(e).__name__}: {e}'[:200])
        uebernehmen('solar_n', 'speicher_n', 'z_solar', 'z_speicher')

    if not any(k.get('solar_n') or k.get('wind_n') or k.get('bev') for k in kreise.values()):
        raise RuntimeError('Vor Ort: keine Kennzahl abrufbar; ' + '; '.join(fehler[:4]))

    offen = sum(1 for k in kreise.values() for a in ('solar', 'speicher') if k.get(f'{a}_n') is None)
    return {
        'data': {
            'kreise': kreise,
            'wind_see': see,
            'stand': stand,
            'offen': offen,
            'hinweise': fehler,
            'quellen': {
                'kreise': 'Statistisches Bundesamt, Gemeindeverzeichnis',
                'mastr': 'Marktstammdatenregister (Bundesnetzagentur)',
                'lade': 'Ladesäulenregister (Bundesnetzagentur)',
                'kba': 'Kraftfahrt-Bundesamt, FZ1',
                'geo': 'Eurostat GISCO NUTS 2024, © EuroGeographics',
            },
        },
        'meta': {
            'source': 'Destatis, Marktstammdatenregister, Ladesäulenregister, Kraftfahrt-Bundesamt',
            'note': 'Zählung je Kreis rollierend; Wind, Ladesäulen und KBA wöchentlich.',
        },
    }
