"""
Gesamtwirtschaftliche Reihen mit Energiebezug – direkt aus den Originalquellen.

  Strom-Großhandel (Monatsmittel) und Stromverbrauch   SMARD / Bundesnetzagentur
  Verbraucherpreise insgesamt und Energie (VPI)        Statistisches Bundesamt,
  Erzeugerpreise gewerblicher Produkte, Einfuhrpreise   abgerufen über die
                                                        Zeitreihen-API der Deutschen
                                                        Bundesbank (ohne Schlüssel)
  Endenergieverbrauch nach Energieträger und Sektor    Eurostat, Energiebilanz nrg_bal_c
  Preiserwartungen, Industrievertrauen                 EU-Kommission, Konjunkturumfragen
                                                        (über Eurostat)
  Super 95, Diesel, Heizöl (Wochenwerte seit 2005)     EU-Kommission, Weekly Oil Bulletin
  Installierte Leistung nach Träger                    Energy-Charts (Fraunhofer ISE)

Ausgabe wie gehabt: data.series[<id>] = {label, unit, group, quelle, series:[{date, v}]}.
Jede Quelle ist einzeln abgesichert; fällt eine aus, bleiben die anderen.
"""
import csv
import io
import re
from datetime import datetime, timezone
from typing import Dict, List

from core import http

EUROSTAT = 'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/'
BUBA = 'https://api.statistiken.bundesbank.de/rest/data/BBDP1/'
SMARD = 'https://www.smard.de/app/chart_data/'
OIL_PAGE = 'https://energy.ec.europa.eu/data-and-analysis/weekly-oil-bulletin_en'
EC_INSTALLED = 'https://api.energy-charts.info/installed_power'


def _pts(pairs) -> List[dict]:
    return [{'date': d, 'v': round(v, 4)} for d, v in sorted(pairs) if v is not None]


def _yoy(monat: Dict[str, float]) -> Dict[str, float]:
    """Veränderung zum Vorjahresmonat in % aus einer Monatsreihe {'YYYY-MM': wert}."""
    out = {}
    for m, v in monat.items():
        vj = monat.get(f'{int(m[:4]) - 1}{m[4:]}')
        if vj:
            out[m] = (v / vj - 1) * 100
    return out


# ── Statistisches Bundesamt über die Bundesbank-API ────────────────────────
def _buba(key: str) -> Dict[str, float]:
    r = http.get_session().get(BUBA + key, params={'format': 'sdmx_csv', 'lang': 'de', 'startPeriod': '2004-01'},
                               timeout=40)
    r.raise_for_status()
    out = {}
    for row in csv.DictReader(io.StringIO(r.content.decode('utf-8-sig')), delimiter=';'):
        t, v = row.get('TIME_PERIOD'), row.get('OBS_VALUE')
        if t and v not in (None, ''):
            out[t[:7]] = float(v.replace(',', '.'))
    if not out:
        raise RuntimeError(f'Bundesbank {key}: keine Werte')
    return out


# ── Eurostat ───────────────────────────────────────────────────────────────
def _eurostat(dataset: str, **flt) -> Dict[str, float]:
    r = http.get_session().get(EUROSTAT + dataset, params={'geo': 'DE', **flt}, timeout=60)
    r.raise_for_status()
    d = r.json()
    zeiten = d['dimension']['time']['category']['index']          # {zeit: position}
    pos = {p: t for t, p in zeiten.items()}
    return {pos[int(k) % len(zeiten)]: float(v) for k, v in (d.get('value') or {}).items()}


# ── SMARD ──────────────────────────────────────────────────────────────────
def _smard_monat(filter_id: int) -> Dict[str, List[float]]:
    s = http.get_session()
    idx = s.get(f'{SMARD}{filter_id}/DE/index_month.json', timeout=30).json().get('timestamps') or []
    werte: Dict[str, List[float]] = {}
    for ts in idx:
        rr = s.get(f'{SMARD}{filter_id}/DE/{filter_id}_DE_month_{ts}.json', timeout=30)
        if not rr.ok:
            continue
        for t, v in rr.json().get('series') or []:
            if v is None:
                continue
            # Zeitstempel = Mitternacht deutscher Zeit; ein paar Stunden später ist sicher im richtigen Monat
            m = datetime.fromtimestamp(t / 1000 + 6 * 3600, timezone.utc).strftime('%Y-%m')
            werte.setdefault(m, []).append(float(v))
    return werte


# ── Weekly Oil Bulletin ────────────────────────────────────────────────────
def _oil_bulletin() -> Dict[str, List]:
    import openpyxl
    s = http.get_session()
    seite = s.get(OIL_PAGE, timeout=40).text
    m = re.search(r'href="([^"]*Prices_History[^"]*\.xlsx)"', seite)
    if not m:
        raise RuntimeError('Oil Bulletin: Download-Link nicht gefunden')
    url = m.group(1) if m.group(1).startswith('http') else 'https://energy.ec.europa.eu' + m.group(1)
    xl = s.get(url, timeout=120)
    xl.raise_for_status()
    ws = openpyxl.load_workbook(io.BytesIO(xl.content), read_only=True, data_only=True)['Prices with taxes']
    zeilen = list(ws.iter_rows(values_only=True))
    kopf = zeilen[0]
    spalte = {k: kopf.index(k) for k in ('DE_price_with_tax_euro95', 'DE_price_with_tax_diesel',
                                         'DE_price_with_tax_heating_oil') if k in kopf}
    if len(spalte) < 3:
        raise RuntimeError('Oil Bulletin: Spalten für Deutschland fehlen')
    out = {k: [] for k in spalte}
    for z in zeilen[3:]:
        if not isinstance(z[0], datetime):
            continue
        d = z[0].strftime('%Y-%m-%d')
        for k, i in spalte.items():
            v = z[i]
            if isinstance(v, (int, float)) and 300 < v < 5000:          # € je 1000 Liter
                out[k].append((d, v / 1000))
    return out


def fetch() -> dict:
    out: Dict[str, dict] = {}
    fehler = []

    def add(sid, label, unit, group, quelle, pairs):
        p = _pts(pairs)
        if p:
            out[sid] = {'label': label, 'unit': unit, 'group': group, 'quelle': quelle, 'series': p}

    # Strom: SMARD
    try:
        # Monatsauflösung: ein Wert je Monat (Preis = Monatsmittel, Last = Monatssumme MWh);
        # der laufende Monat ist unvollständig und fällt weg
        jetzt = datetime.now(timezone.utc).strftime('%Y-%m')
        preis = {m: v[-1] for m, v in _smard_monat(4169).items() if m < jetzt}
        add('de_power_price', 'Großhandelspreis Strom (Monatsmittel)', '€/MWh', 'strom', 'SMARD / Bundesnetzagentur',
            ((m + '-01', v) for m, v in preis.items()))
        last = {m: v[-1] for m, v in _smard_monat(410).items() if m < jetzt}
        add('de_power_load_yoy', 'Stromverbrauch', '% zum Vorjahresmonat', 'strom', 'SMARD / Bundesnetzagentur',
            ((m + '-01', v) for m, v in _yoy(last).items()))
    except Exception as e:
        fehler.append(f'SMARD: {type(e).__name__}: {e}')

    # Preise: Statistisches Bundesamt (über Bundesbank)
    for sid, key, label in (
            ('de_cpi_yoy', 'M.DE.N.VPI.C.A00000.I20.A', 'Verbraucherpreise insgesamt'),
            ('de_vpi_energy_yoy', 'M.DE.N.VPI.C.NRGY00.I20.A', 'Verbraucherpreise Energie'),
            ('de_ppi_yoy', 'M.DE.N.EPG.G.GP19SA000000.I21.A', 'Erzeugerpreise gewerblicher Produkte'),
            ('de_import_prices_yoy', 'M.DE.N.APE1.G.GP19SA000000.I21.A', 'Einfuhrpreise')):
        try:
            add(sid, label, '% zum Vorjahr', 'preise', 'Statistisches Bundesamt (über Deutsche Bundesbank)',
                ((m + '-01', v) for m, v in _yoy(_buba(key)).items()))
        except Exception as e:
            fehler.append(f'{sid}: {type(e).__name__}: {e}')

    # Endenergie: Eurostat-Energiebilanz (GWh -> TWh)
    for sid, siec, label in (('de_fec_o4000xbio', 'O4000XBIO', 'Öl'), ('de_fec_g3000', 'G3000', 'Erdgas'),
                             ('de_fec_e7000', 'E7000', 'Strom'), ('de_fec_h8000', 'H8000', 'Fernwärme'),
                             ('de_fec_ra000', 'RA000', 'Erneuerbare direkt'),
                             ('de_fec_kohle', 'C0000X0350-0370+C0350-0370', 'Kohle (inkl. Kokerei- und Hochofengas)')):
        try:
            teile = [_eurostat('nrg_bal_c', nrg_bal='FC_E', siec=x, unit='GWH') for x in siec.split('+')]
            jahre = set.intersection(*(set(t) for t in teile))
            add(sid, label, 'TWh', 'endenergie_quelle', 'Eurostat, Energiebilanz',
                ((j + '-01-01', sum(t[j] for t in teile) / 1000) for j in jahre))
        except Exception as e:
            fehler.append(f'{sid}: {type(e).__name__}')
    for sid, bal, label in (('de_fec_sek_fc_ind_e', 'FC_IND_E', 'Industrie'), ('de_fec_sek_fc_tra_e', 'FC_TRA_E', 'Verkehr'),
                            ('de_fec_sek_fc_oth_hh_e', 'FC_OTH_HH_E', 'Haushalte'),
                            ('de_fec_sek_fc_oth_cp_e', 'FC_OTH_CP_E', 'Gewerbe, Handel, Dienstleistungen')):
        try:
            add(sid, label, 'TWh', 'endenergie_sektor', 'Eurostat, Energiebilanz',
                ((j + '-01-01', v / 1000) for j, v in _eurostat('nrg_bal_c', nrg_bal=bal, siec='TOTAL', unit='GWH').items()))
        except Exception as e:
            fehler.append(f'{sid}: {type(e).__name__}')

    # Konjunkturumfragen der EU-Kommission (über Eurostat)
    for sid, ds, indic, label in (('de_pexp_ind', 'ei_bsin_m_r2', 'BS-ISPE', 'Preiserwartungen Industrie'),
                                  ('de_pexp_cons', 'ei_bsco_m', 'BS-PT-NY', 'Preiserwartungen Verbraucher'),
                                  ('de_conf_industry', 'ei_bssi_m_r2', 'BS-ICI-BAL', 'Vertrauen Industrie')):
        try:
            add(sid, label, 'Saldo', 'stimmung', 'EU-Kommission, Konjunkturumfragen',
                ((t + '-01', v) for t, v in _eurostat(ds, indic=indic, s_adj='SA', sinceTimePeriod='2005-01').items()))
        except Exception as e:
            fehler.append(f'{sid}: {type(e).__name__}')

    # Kraftstoffe: Weekly Oil Bulletin der EU-Kommission
    try:
        wob = _oil_bulletin()
        for sid, k, label in (('de_fuel_e95', 'DE_price_with_tax_euro95', 'Super 95 (Wochenwert)'),
                              ('de_fuel_diesel', 'DE_price_with_tax_diesel', 'Diesel (Wochenwert)'),
                              ('de_fuel_heating', 'DE_price_with_tax_heating_oil', 'Heizöl (Wochenwert)')):
            add(sid, label, '€/Liter', 'kraftstoff', 'EU-Kommission, Weekly Oil Bulletin', wob[k])
    except Exception as e:
        fehler.append(f'Oil Bulletin: {type(e).__name__}: {e}')

    # Installierte Leistung: Energy-Charts
    try:
        d = http.get_session().get(EC_INSTALLED, params={'country': 'de', 'time_step': 'yearly',
                                                         'installation_decommission': 'false'}, timeout=40).json()
        jahre = d.get('time') or []
        typ = {p['name']: p.get('data') or [] for p in d.get('production_types', [])}

        def summe(namen, i):
            w = [typ.get(n, [None] * len(jahre))[i] for n in namen]
            w = [x for x in w if x is not None]
            return sum(w) if w else None
        bis = datetime.now(timezone.utc).year
        gruppen = (('de_cap_ra420', ['Solar DC'], 'Photovoltaik'),
                   ('de_cap_ra300', ['Wind onshore', 'Wind offshore'], 'Wind'),
                   ('de_cap_cf', ['Fossil brown coal / lignite', 'Fossil hard coal', 'Fossil gas', 'Fossil oil',
                                  'Other, non-renewable', 'Biomass'], 'Kohle, Gas, Öl, Biomasse'),
                   ('de_cap_n9000', ['Nuclear'], 'Kernkraft'))
        for sid, namen, label in gruppen:
            add(sid, label, 'GW', 'leistung', 'Energy-Charts (Fraunhofer ISE)',
                ((f'{j}-01-01', summe(namen, i)) for i, j in enumerate(jahre) if int(j) <= bis))
    except Exception as e:
        fehler.append(f'Energy-Charts Leistung: {type(e).__name__}')

    if not out:
        raise RuntimeError('Makroreihen: nichts geladen; ' + '; '.join(fehler[:5]))
    return {
        'data': {'series': out, 'errors': fehler},
        'meta': {'source': 'SMARD, Statistisches Bundesamt (über Deutsche Bundesbank), Eurostat, '
                           'EU-Kommission (Konjunkturumfragen, Weekly Oil Bulletin), Energy-Charts'},
    }
