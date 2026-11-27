"""
Energiethemen im Trend und amtliche Meldungen.

THEMEN
  Je Thema eine Google-News-Suche (Deutsch, letzte 7 Tage). Gezählt werden
  die Treffer je Tag; die RSS-Antwort enthält höchstens 100 Einträge, bei
  sehr großen Themen ist die Zahl also gedeckelt (Feld "gedeckelt").
  Die Tagessumme der letzten 7 Tage je Thema wird zusätzlich in
  data/history/themen.jsonl fortgeschrieben – daraus wird über Wochen ein
  belastbarer Trend.

AMTLICHE MELDUNGEN
  Pressemitteilungen von Bundesbehörden, Übertragungsnetzbetreibern, EU und
  Verbänden. Wo es einen stabilen RSS-Feed gibt, wird er direkt gelesen;
  sonst eine Google-News-Suche mit site:-Filter, bei der nur Einträge
  bleiben, deren Quelle wirklich die Domain der Stelle ist. Allgemeine
  Stellen (Statistik, Umwelt, EU-Presse) werden auf Energiebezug gefiltert.

Keine Schlüssel nötig.
"""
import html as _html
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional
from urllib.parse import urlparse

from core import http, history, store

GN = 'https://news.google.com/rss/search'

# id, Name, Suchanfrage, Reiter im Dashboard
THEMEN = [
    ('strompreis',   'Strompreis',                 'Strompreis OR Strompreise',                 'strompreise'),
    ('netzentgelte', 'Netzentgelte',               'Netzentgelte',                              'strompreise'),
    ('stromsteuer',  'Stromsteuer & Entlastung',   'Stromsteuer OR Strompreisbremse',           'strompreise'),
    ('industrie',    'Industriestrompreis',        'Industriestrompreis',                       'jahr'),
    ('gasspeicher',  'Gasspeicher & Versorgung',   'Gasspeicher OR Gasversorgung',              'gas'),
    ('gaspreis',     'Gaspreis',                   'Gaspreis OR Gaspreise',                     'gas'),
    ('co2',          'CO₂-Preis & Emissionshandel', '"CO2-Preis" OR Emissionshandel',           'umwelt'),
    ('heizung',      'Heizungsgesetz & Wärmepumpe', 'Heizungsgesetz OR Gebäudeenergiegesetz OR Wärmepumpe', 'umwelt'),
    ('kraftwerke',   'Gaskraftwerke & Kraftwerksstrategie', 'Gaskraftwerke OR Kraftwerksstrategie', 'strom'),
    ('wasserstoff',  'Wasserstoff',                'Wasserstoff Energie',                       'futures'),
    ('lng',          'LNG & Flüssiggas',           'LNG-Terminal OR Flüssiggas',                'gas'),
    ('wind',         'Windkraft',                  'Windkraft OR Windenergie',                  'erneuerbare'),
    ('solar',        'Solar & Photovoltaik',       'Photovoltaik OR Solarstrom',                'erneuerbare'),
    ('netzausbau',   'Netzausbau & Stromnetz',     'Netzausbau OR Stromnetz',                   'strom'),
    ('sprit',        'Spritpreise',                'Spritpreise OR Benzinpreis OR Dieselpreis', 'kraftstoff'),
    ('heizoel',      'Heizöl',                     'Heizölpreis OR Heizöl',                     'kraftstoff'),
    ('atom',         'Kernkraft',                  'Atomkraft OR Kernkraft Deutschland',        'strom'),
    ('oel',          'Ölpreis & OPEC',             'Ölpreis OR OPEC',                           'rohstoffe'),
]

# Name, Gruppe, Art ('rss' oder 'gn'), Adresse bzw. Domain, Energiefilter
STELLEN = [
    ('Bundesnetzagentur', 'Bund', 'rss',
     'https://www.bundesnetzagentur.de/SiteGlobals/Functions/RSSFeed/DE/RSSNewsfeed/RSSNewsfeed_Pressemitteilungen.xml?nn=265778', True),
    ('Bundeswirtschaftsministerium', 'Bund', 'gn', 'bundeswirtschaftsministerium.de', True),
    ('Bundesregierung', 'Bund', 'gn', 'bundesregierung.de', True),
    ('Statistisches Bundesamt', 'Bund', 'gn', 'destatis.de', True),
    ('Umweltbundesamt', 'Bund', 'gn', 'umweltbundesamt.de', True),
    ('Amprion', 'Netzbetreiber', 'gn', 'amprion.net', False),
    ('50Hertz', 'Netzbetreiber', 'gn', '50hertz.com', False),
    ('TenneT', 'Netzbetreiber', 'gn', 'tennet.eu', False),
    ('TransnetBW', 'Netzbetreiber', 'gn', 'transnetbw.de', False),
    ('EU-Kommission Energie', 'EU', 'rss', 'https://energy.ec.europa.eu/node/2/rss_en', False),
    ('EU-Kommission Presse', 'EU', 'rss', 'https://ec.europa.eu/commission/presscorner/api/rss?language=de', True),
    ('dena', 'Agenturen & Verbände', 'gn', 'dena.de', False),
    ('BDEW', 'Agenturen & Verbände', 'gn', 'bdew.de', False),
]

# Stellenanzeigen, Veranstaltungshinweise und Ähnliches gehören nicht in die Lage
RAUSCHEN_RE = re.compile(r'm/w/d|w/m/d|\(m/w|webinar|praktikum|werkstudent|ausbildung|stellenangebot|karriere|'
                         r'job\b|veranstaltung|anmeldung|save the date|telekommunikation|mobilfunk|porto|porti|post\b|eisenbahn|'
                         r'frequenz|datenvermittlung', re.I)

# Themenzuordnung der Meldungen über Wortstämme
THEMA_RE = {
    'strompreis': r'strompreis|stromkosten|stromrechnung|stromtarif',
    'netzentgelte': r'netzentgelt|netzkosten|wacc',
    'stromsteuer': r'stromsteuer|entlastung|preisbremse|umlage',
    'industrie': r'industriestrom|energieintensiv|strompreiskompensation',
    'gasspeicher': r'gasspeicher|speicherfüll|gasversorgung|versorgungssicherheit|winter',
    'gaspreis': r'gaspreis|gasmarkt|gaspaket|gasnetz|gas- und strompreis',
    'co2': r'co2|co₂|emission|ets\b|klimaschutz|treibhaus',
    'heizung': r'heizung|gebäudeenergie|wärmepumpe|wärmeplan|wärmenetz|fernwärme|abwärme',
    'kraftwerke': r'kraftwerk|kapazit|versorgungssicherheit|dunkelflaute',
    'wasserstoff': r'wasserstoff|hydrogen|elektrolys',
    'lng': r'\blng\b|flüssiggas',
    'wind': r'wind',
    'solar': r'solar|photovolt|\bpv\b',
    'netzausbau': r'netzausbau|stromnetz|leitung|umspann|netzanschluss|übertragungsnetz|grid|kabel|konverter|speicher',
    'sprit': r'kraftstoff|benzin|diesel|tankstelle|sprit',
    'heizoel': r'heizöl',
    'atom': r'kernkraft|atom|nuclear|kernenergie',
    'oel': r'ölpreis|rohöl|opec|\boil\b',
}

ENERGIE_RE = re.compile(
    r'energ|strom|gas\b|gase\b|erdgas|gasmarkt|gaspaket|wasserstoff|kraftwerk|stromnetz|gasnetz|wärmenetz|netzausbau|netzentgelt|'
    r'übertragungsnetz|wind|solar|photovolt|wärme|heiz|'
    r'kohle|öl\b|ölpreis|kraftstoff|benzin|diesel|co2|co₂|emission|klimaschutz|lng|batterie|speicher|'
    r'elektri|power|electric|grid|hydrogen|renewable|fuel|oil\b|nuclear|kern|atom', re.I)


def _datum(txt: Optional[str]) -> Optional[datetime]:
    if not txt:
        return None
    try:
        d = parsedate_to_datetime(txt)
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    try:
        return datetime.fromisoformat(txt.replace('Z', '+00:00'))
    except Exception:
        return None


def _sauber(t: str) -> str:
    t = _html.unescape(re.sub(r'<[^>]+>', ' ', t or ''))
    return re.sub(r'\s+', ' ', t).strip()


def _text(beschreibung: str, titel: str) -> str:
    """Ausschnitt aus der Feed-Beschreibung – leer, wenn sie nur die Überschrift wiederholt."""
    t, ti = _sauber(beschreibung), _sauber(titel)
    ti = re.sub(r'\s+[-–|]\s+[^-–|]+$', '', ti)          # " - Quelle" am Ende der Überschrift
    if not t or len(t) < 40 or t.startswith(ti[:30]) or ti.startswith(t[:30]):
        return ''
    return t[:600]


def _erster_absatz(url: str) -> str:
    """Erster Absatz nach der Überschrift einer Pressemitteilung (Bundesnetzagentur)."""
    r = http.get_session().get(url.replace('http://', 'https://'), timeout=20)
    r.raise_for_status()
    h = r.text
    i = h.find('<h1')
    for p in re.findall(r'<p[^>]*>(.*?)</p>', h[i if i >= 0 else 0:], re.S):
        t = _sauber(p)
        if len(t) > 80:
            return t[:700]
    return ''


def _rss(url: str, params: Optional[dict] = None) -> List[dict]:
    r = http.get_session().get(url, params=params, timeout=20)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    out = []
    for it in root.iter('item'):
        src = it.find('source')
        link = (it.findtext('link') or '').strip()
        datum = _datum(it.findtext('pubDate'))
        if datum is None:                     # z. B. Bundesnetzagentur: Datum nur in der Adresse
            m = re.search(r'/(20\d{6})_', link)
            if m:
                datum = datetime.strptime(m.group(1), '%Y%m%d').replace(tzinfo=timezone.utc, hour=12)
        out.append({
            'text': _text(it.findtext('description') or '', it.findtext('title') or ''),
            'titel': (it.findtext('title') or '').strip(),
            'link': link,
            'datum': datum,
            'quelle': (src.text or '').strip() if src is not None else '',
            'quelle_url': src.get('url', '') if src is not None else '',
        })
    return out


def _gn(query: str) -> List[dict]:
    return _rss(GN, {'q': query, 'hl': 'de', 'gl': 'DE', 'ceid': 'DE:de'})


def _titel_ohne_quelle(t: str, quelle: str) -> str:
    if quelle and t.endswith(' - ' + quelle):
        return t[: -len(quelle) - 3].strip()
    return t


def _themen_von(text: str) -> List[str]:
    return [tid for tid, rx in THEMA_RE.items() if re.search(rx, text, re.I)]


def fetch() -> dict:
    jetzt = datetime.now(timezone.utc)
    tage = [(jetzt - timedelta(days=i)).date().isoformat() for i in range(6, -1, -1)]
    themen = []
    fehler = []
    for tid, name, q, tab in THEMEN:
        try:
            items = _gn(f'{q} when:7d')
        except Exception as e:
            fehler.append(f'{tid}: {type(e).__name__}')
            continue
        je_tag: Dict[str, int] = {d: 0 for d in tage}
        for it in items:
            if it['datum']:
                d = it['datum'].date().isoformat()
                if d in je_tag:
                    je_tag[d] += 1
        items = sorted([i for i in items if i['datum']], key=lambda i: i['datum'], reverse=True)
        themen.append({
            'id': tid, 'name': name, 'tab': tab,
            'tage': [je_tag[d] for d in tage],
            'summe_7t': sum(je_tag.values()),
            'gedeckelt': len(items) >= 100,
            'schlagzeilen': [{'titel': _titel_ohne_quelle(i['titel'], i['quelle']), 'quelle': i['quelle'],
                              'link': i['link'], 'datum': i['datum'].isoformat()} for i in items[:3]],
        })
        time.sleep(0.8)

    meldungen = []
    grenze = jetzt - timedelta(days=45)
    for name, gruppe, art, ziel, filtern in STELLEN:
        try:
            if art == 'rss':
                items = _rss(ziel)
            else:
                items = _gn(f'site:{ziel} when:30d')
                items = [i for i in items if ziel in urlparse(i['quelle_url']).netloc]
                for i in items:                   # die Suche liefert nie einen Text, nur die Überschrift
                    i['text'] = ''
        except Exception as e:
            fehler.append(f'{name}: {type(e).__name__}')
            continue
        for i in items:
            titel = _titel_ohne_quelle(i['titel'], i['quelle'])
            if not titel or not i['datum'] or i['datum'] < grenze:
                continue
            if filtern and not ENERGIE_RE.search(titel):
                continue
            if RAUSCHEN_RE.search(titel):
                continue
            # reine Seitentitel ("Soziale Netzwerke", "… | BMWE") sind keine Meldungen
            if len(titel.split()) < 3 or titel.startswith('-') or re.search(r'\|\s*\w+$', titel):
                continue
            meldungen.append({'stelle': name, 'gruppe': gruppe, 'titel': titel, 'link': i['link'],
                              'datum': i['datum'].isoformat(), 'themen': _themen_von(titel + ' ' + i.get('text', '')),
                              'text': i.get('text', '')})
        time.sleep(0.8)

    if not themen and not meldungen:
        raise RuntimeError('Themen/Meldungen: nichts geladen; ' + '; '.join(fehler[:5]))

    # doppelte Titel (gleiche Meldung über RSS und Suche) nur einmal
    gesehen, eindeutig = set(), []
    for m in sorted(meldungen, key=lambda m: m['datum'], reverse=True):
        k = re.sub(r'\W+', '', m['titel'].lower())[:80]
        if k in gesehen:
            continue
        gesehen.add(k)
        eindeutig.append(m)

    # Ausschnitte: Pressemitteilungen der Bundesnetzagentur haben keinen Text im
    # Feed; der erste Absatz der Seite wird nachgeladen und zwischengespeichert.
    alt = {m.get('link'): m.get('text') for m in ((store.read_json('themen') or {}).get('data') or {}).get('meldungen', [])
           if m.get('text')}
    nachgeladen = 0
    for m in eindeutig[:200]:
        if m['text'] or 'bundesnetzagentur.de' not in m['link']:
            continue
        if alt.get(m['link']):
            m['text'] = alt[m['link']]
        elif nachgeladen < 12:
            try:
                m['text'] = _erster_absatz(m['link'])
                nachgeladen += 1
                time.sleep(0.5)
            except Exception as e:
                fehler.append(f'Text {m["link"][-40:]}: {type(e).__name__}')
        if m['text'] and not m['themen']:
            m['themen'] = _themen_von(m['titel'] + ' ' + m['text'])

    # Fachartikel mit echtem Text aus dem News-Abruf des Dashboards
    artikel = []
    for a in ((store.read_json('news') or {}).get('data') or {}).get('articles', []):
        text = _text(a.get('summary') or '', a.get('title') or '')
        titel = _sauber(a.get('title') or '')
        if not text or not ENERGIE_RE.search(titel + ' ' + text) or RAUSCHEN_RE.search(titel):
            continue
        th = _themen_von(titel + ' ' + text)
        artikel.append({'titel': titel, 'quelle': a.get('source') or '', 'link': a.get('link') or '',
                        'datum': a.get('date') or '', 'text': text, 'themen': th})
    artikel.sort(key=lambda a: a['datum'], reverse=True)

    history.record_history('themen', {t['id']: t['summe_7t'] for t in themen})

    return {
        'data': {'themen': themen, 'tage': tage, 'meldungen': eindeutig[:200], 'artikel': artikel[:80],
                 'fehler': fehler},
        'meta': {
            'source': 'Google News (Suche je Thema, Deutsch), Pressestellen von Bund, '
                      'Übertragungsnetzbetreibern, EU und Verbänden',
            'note': 'Themenzahlen sind Treffer der Nachrichtensuche, je Abfrage höchstens 100.',
        },
    }
