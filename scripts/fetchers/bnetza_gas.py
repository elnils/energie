"""
Bundesnetzagentur — "Aktuelle Lage der Gasversorgung".

The BNetzA publishes every chart on its gas-supply page as a CSV next to the
SVG: imports, LNG, exports, domestic production, prices, storage, and
consumption split into industry (RLM) and households/commerce (SLP), plus
the temperature that drives it. Each CSV carries the full history behind the
chart, so one fetch is a complete backfill.

Why the parser is generic
-------------------------
The files do not share a layout. Daily series come as one row per date with
one column per border point or country; the weekly and monthly consumption
files come as one row per week/month with one column per YEAR (that is how
the page draws its "compared with previous years" lines). Headers and units
change without notice. So instead of one parser per file this module:

  1. decodes (utf-8-sig, then cp1252) and sniffs the delimiter,
  2. finds the header row (skips title lines above it),
  3. classifies the first column as date / month / week / label,
  4. turns every other column into a series of [x, value] pairs,
  5. flags `years_as_columns` when the headers are years.

The first raw lines of every file are logged so a layout change is visible in
the next Actions log without re-running anything.

Discovery
---------
The page's dropdowns switch between CSVs. Besides the known list, every
`_svg/<Dir>/<Dir>.html` page and the landing page are scanned for CSV links;
any new one is fetched too and recorded in meta.discovered.

No data is ever dropped: each run is merged into the previous file by x value,
so a CSV that is shortened upstream (the BNetzA rolls some charts) cannot
erase the history already committed here.
"""
import csv
import io
import re
from datetime import date
from typing import Dict, List, Optional, Tuple

from core import http, store

BASE = 'https://www.bundesnetzagentur.de/DE/Gasversorgung/aktuelle_gasversorgung/_svg/'
LANDING = 'https://www.bundesnetzagentur.de/DE/Gasversorgung/aktuelle_gasversorgung/start.html'

# key -> (path below BASE, label, group, unit, frequency)
DATASETS: Dict[str, Tuple[str, str, str, str, str]] = {
    'import_gesamt':  ('Gasimporte/Gasimporte_CSV.csv',
                       'Gasimporte nach Herkunft', 'Importe & Exporte', 'GWh/Tag', 'daily'),
    'import_lng':     ('Gasimporte_LNG/Gasimporte_LNG_CSV.csv',
                       'LNG-Terminals', 'Importe & Exporte', 'GWh/Tag', 'daily'),
    'export_gesamt':  ('Gasexporte/Gasexporte_CSV.csv',
                       'Gasexporte in Nachbarländer', 'Importe & Exporte', 'GWh/Tag', 'daily'),
    'foerderung':     ('Foerderung/Foerderung_CSV.csv',
                       'Inländische Förderung', 'Förderung & Preise', 'GWh/Tag', 'daily'),
    'preise':         ('Gaspreise/Gaspreise_CSV.csv',
                       'Großhandelspreise', 'Förderung & Preise', 'EUR/MWh', 'daily'),
    'speicher_fuellstand': ('Gasspeicher_Fuellstand/Speicherfuellstand_CSV.csv',
                       'Speicherfüllstand', 'Speicher', '%', 'daily'),
    'speicher_veraenderung': ('Gasspeicher_Verainderung/Speicher_Veraenderung_CSV.csv',
                       'Tägliche Speicherveränderung', 'Speicher', '%-Punkte', 'daily'),
    'verbrauch_gesamt_woche': ('Gasverbrauch_Gesamt_woechentlich/Gasverbrauch_Gesamt_W_2023_CSV.csv',
                       'Gesamtverbrauch (Wochenmittel)', 'Verbrauch gesamt', 'GWh/Tag', 'weekly'),
    'verbrauch_gesamt_monat': ('Gasverbrauch_Gesamt_monatlich/Gasverbrauch_Gesamt_M_2023_CSV.csv',
                       'Gesamtverbrauch (Monatsmittel)', 'Verbrauch gesamt', 'GWh/Tag', 'monthly'),
    'verbrauch_industrie_woche': ('Gasverbrauch_Industrie_woechentlich/Gasverbrauch_Industrie_W_CSV.csv',
                       'Industrie RLM (Wochenmittel)', 'Industrie (RLM)', 'GWh/Tag', 'weekly'),
    'verbrauch_industrie_monat': ('Gasverbrauch_Industrie_monatlich/Gasverbrauch_Industrie_M_CSV.csv',
                       'Industrie RLM (Monatsmittel)', 'Industrie (RLM)', 'GWh/Tag', 'monthly'),
    'veraenderung_industrie_woche': ('Gasverbrauch_Veraenderung_RLM_woechentlich/Gasverbrauch_Veraenderung_RLM_W_CSV.csv',
                       'Industrie: Veränderung (Woche)', 'Industrie (RLM)', '% ggü. Referenz', 'weekly'),
    'veraenderung_industrie_monat': ('Gasverbrauch_Veraenderung_RLM_monatlich/Gasverbrauch_Veraenderung_RLM_M_CSV.csv',
                       'Industrie: Veränderung (Monat)', 'Industrie (RLM)', '% ggü. Referenz', 'monthly'),
    'verbrauch_haushalt_woche': ('Gasverbrauch_Haushalte_woechentlich/Gasverbrauch_Haushalte_W_CSV.csv',
                       'Haushalte & Gewerbe SLP (Wochenmittel)', 'Haushalte & Gewerbe (SLP)', 'GWh/Tag', 'weekly'),
    'verbrauch_haushalt_monat': ('Gasverbrauch_Haushalte_monatlich/Gasverbrauch_Haushalte_M_CSV.csv',
                       'Haushalte & Gewerbe SLP (Monatsmittel)', 'Haushalte & Gewerbe (SLP)', 'GWh/Tag', 'monthly'),
    'veraenderung_haushalt_woche': ('Gasverbrauch_Veraenderung_SLP_woechentlich/Gasverbrauch_Veraenderung_SLP_W_CSV.csv',
                       'Haushalte: Veränderung (Woche)', 'Haushalte & Gewerbe (SLP)', '% ggü. Referenz', 'weekly'),
    'veraenderung_haushalt_monat': ('Gasverbrauch_Veraenderung_SLP_monatlich/Gasverbrauch_Veraenderung_SLP_M_CSV.csv',
                       'Haushalte: Veränderung (Monat)', 'Haushalte & Gewerbe (SLP)', '% ggü. Referenz', 'monthly'),
    'temperatur_woche': ('Temperatur_woechentlich/Temperatur_W_CSV.csv',
                       'Temperatur (Wochenmittel)', 'Temperatur', '°C', 'weekly'),
    'temperatur_monat': ('Temperatur_monatlich/Temperatur_M_CSV.csv',
                       'Temperatur (Monatsmittel)', 'Temperatur', '°C', 'monthly'),
}

MONTHS_DE = {
    'jan': 1, 'januar': 1, 'feb': 2, 'februar': 2, 'mär': 3, 'maer': 3, 'mrz': 3,
    'märz': 3, 'maerz': 3, 'mar': 3, 'apr': 4, 'april': 4, 'mai': 5, 'may': 5,
    'jun': 6, 'juni': 6, 'jul': 7, 'juli': 7, 'aug': 8, 'august': 8,
    'sep': 9, 'sept': 9, 'september': 9, 'okt': 10, 'oct': 10, 'oktober': 10,
    'nov': 11, 'november': 11, 'dez': 12, 'dec': 12, 'dezember': 12,
}

_YEAR_RE = re.compile(r'^(19|20)\d\d$')
_CSV_LINK_RE = re.compile(r'href="([^"]+?\.csv[^"]*)"', re.I)
_SVG_DIR_RE = re.compile(r'_svg/([A-Za-z0-9_]+)/', re.I)


# ── parsing ─────────────────────────────────────────────────────────────

def _decode(raw: bytes) -> str:
    for enc in ('utf-8-sig', 'cp1252', 'latin-1'):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode('utf-8', errors='replace')


def _num(cell: str) -> Optional[float]:
    """German number: '1.234,5' -> 1234.5, '12,3' -> 12.3, '' / '-' -> None."""
    s = (cell or '').strip().replace(' ', '').replace(' ', '')
    s = s.rstrip('%')
    if not s or s in ('-', '–', 'n.v.', 'k.A.', 'NA', 'nan'):
        return None
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return None


def _parse_x(cell: str) -> Tuple[str, Optional[str]]:
    """Classify one first-column value. Returns (kind, normalised key)."""
    s = (cell or '').strip().strip('"')
    if not s:
        return 'empty', None
    m = re.match(r'^(\d{1,2})\.(\d{1,2})\.(\d{2,4})', s)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        y = y + 2000 if y < 100 else y
        try:
            return 'date', date(y, mo, d).isoformat()
        except ValueError:
            return 'label', s
    # "01.04." without a year: a day in the season, one column per year.
    m = re.match(r'^(\d{1,2})\.(\d{1,2})\.?$', s)
    if m and 1 <= int(m.group(2)) <= 12 and 1 <= int(m.group(1)) <= 31:
        return 'index', f'{int(m.group(2)):02d}-{int(m.group(1)):02d}'
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})', s)
    if m:
        return 'date', f'{m.group(1)}-{m.group(2)}-{m.group(3)}'
    m = re.match(r'^(\d{1,2})[./](\d{4})$', s)
    if m and 1 <= int(m.group(1)) <= 12:
        return 'month', f'{m.group(2)}-{int(m.group(1)):02d}'
    m = re.match(r'^(\d{4})-(\d{2})$', s)
    if m:
        return 'month', s
    # "KW 12", "KW12 2023", "2023 KW 12", "12. KW"
    m = re.search(r'KW\s*(\d{1,2})', s, re.I) or re.match(r'^(\d{1,2})\.\s*KW', s, re.I)
    if m:
        wk = int(m.group(1))
        y = re.search(r'(19|20)\d\d', s)
        # Without a year it is a position in the season, like a bare number.
        return ('week', f'{y.group(0)}-W{wk:02d}') if y else ('index', f'{wk:02d}')
    low = s.lower().rstrip('.')
    parts = re.split(r'[\s./-]+', low)
    if parts and parts[0] in MONTHS_DE:
        mo = MONTHS_DE[parts[0]]
        y = next((p for p in parts[1:] if re.match(r'^\d{2,4}$', p)), None)
        if y:
            yy = int(y) + 2000 if len(y) == 2 else int(y)
            return 'month', f'{yy}-{mo:02d}'
        return 'index', f'{mo:02d}'
    if re.match(r'^\d{1,2}$', s) and 1 <= int(s) <= 53:
        return 'index', f'{int(s):02d}'
    return 'label', s


def parse_csv(text: str) -> dict:
    """Generic BNetzA CSV -> {x_kind, columns, years_as_columns, series}."""
    sample = text[:4000]
    try:
        delim = csv.Sniffer().sniff(sample, delimiters=';,\t').delimiter
    except csv.Error:
        delim = ';'
    rows = [r for r in csv.reader(io.StringIO(text), delimiter=delim)]
    rows = [[c.strip() for c in r] for r in rows if any(c.strip() for c in r)]
    if not rows:
        raise ValueError('empty CSV')

    # Header = the last row before the first row whose cells after the first
    # are mostly numeric. Title lines above it are kept as notes.
    header_idx = 0
    for i, r in enumerate(rows[:15]):
        tail = r[1:]
        if tail and sum(_num(c) is not None for c in tail) >= max(1, len(tail) // 2):
            header_idx = max(0, i - 1)
            break
    header = rows[header_idx]
    notes = [' '.join(c for c in r if c) for r in rows[:header_idx]]
    body = rows[header_idx + 1:]

    columns = []
    for j, h in enumerate(header[1:], start=1):
        name = h or f'Spalte {j}'
        while name in columns:          # duplicate headers happen
            name += '*'
        columns.append(name)

    kinds: Dict[str, int] = {}
    series: Dict[str, Dict[str, float]] = {c: {} for c in columns}
    for r in body:
        kind, x = _parse_x(r[0] if r else '')
        if x is None:
            continue
        kinds[kind] = kinds.get(kind, 0) + 1
        for j, col in enumerate(columns, start=1):
            v = _num(r[j]) if j < len(r) else None
            if v is not None:
                series[col][x] = v
    x_kind = max(kinds, key=kinds.get) if kinds else 'label'
    years_as_columns = bool(columns) and sum(
        bool(_YEAR_RE.match(re.sub(r'\D', '', c)[:4] if c else '')) for c in columns
    ) >= max(2, len(columns) * 0.6)

    return {
        'x_label': header[0] if header else '',
        'x_kind': x_kind,
        'columns': columns,
        'years_as_columns': years_as_columns,
        'notes': [n for n in notes if n][:5],
        'series': {c: sorted(v.items()) for c, v in series.items() if v},
    }


def _merge(prev: Optional[dict], new: dict) -> dict:
    """Union by x per column; new values win. Columns only ever get added."""
    if not prev or not isinstance(prev.get('series'), dict):
        return new
    merged = {}
    cols = list(new['series'].keys())
    cols += [c for c in prev['series'] if c not in new['series']]
    for c in cols:
        d = {x: v for x, v in prev['series'].get(c, [])}
        d.update({x: v for x, v in new['series'].get(c, [])})
        merged[c] = sorted(d.items())
    out = dict(new)
    out['series'] = merged
    out['columns'] = cols
    return out


# ── fetching ────────────────────────────────────────────────────────────

def _get(url: str):
    s = http.get_session()
    r = s.get(url, timeout=30, headers={'Accept': 'text/csv, text/plain, text/html, */*'})
    return r


_QUOTED_RE = re.compile(r'(?:href|src|data|data-src|data-url|content)\s*=\s*["\']([^"\'<>\s]+)["\']', re.I)
_DATA_EXT_RE = re.compile(r'\.(csv|xlsx?|json|txt|zip)(\?|$)', re.I)
_ORIGIN = 'https://www.bundesnetzagentur.de'


def _abs(href: str, page: str) -> str:
    href = href.replace('&amp;', '&')
    if href.startswith('//'):
        return 'https:' + href
    if href.startswith('/'):
        return _ORIGIN + href
    if href.startswith('http'):
        return href
    return page.split('?')[0].rsplit('/', 1)[0] + '/' + href


def _discover(seed_dirs: List[str]) -> Dict[str, str]:
    """
    Walk the landing page and every gas-supply chart page it links, and
    collect links to data files (csv, xlsx, json, txt).

    The chart pages are followed by the hrefs the site itself uses — the
    first version guessed `<Dir>/<Dir>.html` and got 404 for most of them.
    Everything found is logged, so the next Actions run shows how the site
    actually links its data even when no CSV turns up.
    """
    found: Dict[str, str] = {}
    pages = [LANDING, BASE + 'Gasimporte/Gasimporte.html']
    seen = set(pages)
    svgs: List[str] = []
    for i, page in enumerate(pages):      # the list grows while we walk it
        if i >= 45:
            break
        try:
            r = _get(page)
            if not r.ok:
                print(f'    discover {page.replace(_ORIGIN, "")}: HTTP {r.status_code}')
                continue
            html = r.text
        except Exception as e:
            print(f'    discover {page.replace(_ORIGIN, "")}: {type(e).__name__}')
            continue
        links = [_abs(h, page) for h in _QUOTED_RE.findall(html)]
        data = [u for u in links if _DATA_EXT_RE.search(u) or '__blob=publicationFile' in u and 'csv' in u.lower()]
        subpages = [u for u in links
                    if 'aktuelle_gasversorgung' in u and re.search(r'\.html(\?|#|$)', u)
                    and u.split('#')[0] not in seen]
        page_svgs = [u for u in links if re.search(r'\.svg(\?|$)', u, re.I)]
        print(f'    discover {page.replace(_ORIGIN, "")}: {len(html)} B, {len(links)} links, '
              f'{len(data)} data, {len(subpages)} new pages, {len(page_svgs)} svg')
        for u in data[:20]:
            print(f'      data: {u}')
        if i < 2:
            for u in subpages[:40]:
                print(f'      page: {u}')
            # How does the page mention the chart data at all?
            for m in list(re.finditer(r'csv|download|daten', html, re.I))[:6]:
                snippet = re.sub(r'\s+', ' ', html[max(0, m.start() - 90):m.end() + 90])
                print(f'      ctx: …{snippet}…')
        for u in data:
            found[u.split('?')[0]] = u
        for u in subpages:
            u = u.split('#')[0]
            if u not in seen:
                seen.add(u)
                pages.append(u)
        svgs += [u for u in page_svgs if u not in svgs]
    # The charts are SVGs; if they carry their numbers inline, the head of
    # one tells us.
    for u in svgs[:2]:
        try:
            r = _get(u)
            head = re.sub(r'\s+', ' ', r.text[:300])
            print(f'      svg {u.replace(_ORIGIN, "")}: HTTP {r.status_code}, {len(r.text)} B, head: {head}')
        except Exception as e:
            print(f'      svg {u}: {type(e).__name__}')
    return found


def _key_for(url: str) -> str:
    name = url.rsplit('/', 1)[-1]
    name = re.sub(r'(_CSV)?\.csv$', '', name, flags=re.I)
    return 'x_' + re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')


def fetch() -> dict:
    prev = (store.read_json('bnetza_gas') or {}).get('data', {}).get('datasets', {})
    out: Dict[str, dict] = {}
    errors: Dict[str, str] = {}

    seed_dirs = sorted({p.split('/')[0] for p, *_ in DATASETS.values()})
    discovered = _discover(seed_dirs)
    known_urls = {BASE + p for p, *_ in DATASETS.values()}
    extra = {u: full for u, full in discovered.items() if u not in known_urls}
    print(f'    discovered {len(discovered)} CSV links, {len(extra)} not in the known list')
    for u in list(extra)[:25]:
        print(f'      + {extra[u]}')

    # A known file the page links with a query string is fetched that way.
    jobs: List[Tuple[str, str, str, str, str, str]] = [
        (k, discovered.get(BASE + p, BASE + p), lbl, grp, unit, freq)
        for k, (p, lbl, grp, unit, freq) in DATASETS.items()
    ]
    for u in list(extra)[:25]:
        jobs.append((_key_for(u), extra[u], u.rsplit('/', 1)[-1], 'Weitere', '', ''))

    for key, url, label, group, unit, freq in jobs:
        try:
            r = _get(url)
            if not r.ok and '?' not in url:
                # The CMS delivers most downloads only with this parameter.
                r2 = _get(url + '?__blob=publicationFile')
                if r2.ok:
                    url, r = url + '?__blob=publicationFile', r2
            if not r.ok:
                raise RuntimeError(f'HTTP {r.status_code}')
            text = _decode(r.content)
            if text.lstrip().startswith('<'):
                raise RuntimeError('HTML statt CSV erhalten')
            head = text.splitlines()[:3]
            print(f'    {key}: {len(text)} B · first lines: {head}')
            parsed = parse_csv(text)
            if not parsed['series']:
                raise RuntimeError(f'keine Zahlenspalten erkannt (Kopf: {parsed["columns"][:6]})')
            node = _merge(prev.get(key), parsed)
            node.update({'label': label, 'group': group, 'unit': unit, 'freq': freq,
                         'url': url, 'ok': True})
            npts = sum(len(v) for v in node['series'].values())
            xs = [x for v in node['series'].values() for x, _ in v]
            print(f'      -> kind={node["x_kind"]} years_as_cols={node["years_as_columns"]} '
                  f'cols={len(node["columns"])} pts={npts} x={min(xs)}..{max(xs)}')
            out[key] = node
        except Exception as e:
            msg = f'{type(e).__name__}: {e}'[:200]
            print(f'  ! {key}: {msg}')
            errors[key] = msg
            if key in prev:
                # Keep what we had — flagged, never dropped.
                node = dict(prev[key])
                node['ok'] = False
                node['error'] = msg
                out[key] = node
            else:
                out[key] = {'label': label, 'group': group, 'unit': unit, 'freq': freq,
                            'url': url, 'ok': False, 'error': msg, 'series': {}, 'columns': []}

    if not any(n.get('ok') for n in out.values()) and not any(n.get('series') for n in out.values()):
        raise RuntimeError(f'no BNetzA CSV could be read: {list(errors.items())[:3]}')

    return {
        'data': {'datasets': out},
        'meta': {
            'source': 'Bundesnetzagentur — Aktuelle Lage der Gasversorgung',
            'page': 'https://www.bundesnetzagentur.de/DE/Gasversorgung/aktuelle_gasversorgung/start.html',
            'errors': errors,
            'discovered': sorted(discovered)[:80],
            'note': 'Jede CSV enthält die volle Historie; Läufe werden je x-Wert zusammengeführt, nichts wird gelöscht.',
        },
    }
