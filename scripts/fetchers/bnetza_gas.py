"""
Bundesnetzagentur — "Aktuelle Lage der Gasversorgung".

The BNetzA gas-supply pages (imports, LNG, exports, production, prices,
storage, consumption RLM/SLP, temperature) draw their charts with Chart.js
and inline the complete data in the page script:

    const data_myChartId_870296 = { labels: ['01.01.2022', ...],
                                    datasets: [{label: 'Norwegen', data: [...]}, ...] }

No CSV is linked — the CSV names that circulate for these charts all answer
404 (checked in the Actions runs of 2026-09-23). So this module reads the
landing page and every `_svg/.../*.html` chart page it links, pulls each
Chart.js data object out of the scripts, and turns it into series. HTML
tables and linked CSV/XLSX files are still read if the site adds any.

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
import time
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


def parse_csv(text: str, delimiter: Optional[str] = None) -> dict:
    """Generic BNetzA CSV -> {x_kind, columns, years_as_columns, series}."""
    sample = text[:4000]
    try:
        delim = delimiter or csv.Sniffer().sniff(sample, delimiters=';,\t').delimiter
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



# ── HTML pages: tables and inline charts ────────────────────────────────
# The first live runs showed that the gas-supply pages link no CSV at all:
# start.html is 1.3 MB and Gasimporte.html 670 KB, with every chart drawn
# as inline SVG. The numbers therefore sit in the page itself — as data
# tables next to the charts and/or inside the SVG. This part reads both.

from html.parser import HTMLParser


class _TableGrab(HTMLParser):
    """Collect every <table> as rows of cell text, with a nearby title."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tables: List[dict] = []
        self._stack: List[dict] = []
        self._cell: Optional[List[str]] = None
        self._row: Optional[List[str]] = None
        self._heading: Optional[List[str]] = None
        self.last_heading = ''
        self._caption: Optional[List[str]] = None

    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            self._stack.append({'rows': [], 'title': self.last_heading, 'caption': ''})
        elif tag == 'tr' and self._stack:
            self._row = []
        elif tag in ('td', 'th') and self._row is not None:
            self._cell = []
        elif tag == 'caption' and self._stack:
            self._caption = []
        elif tag in ('h1', 'h2', 'h3', 'h4'):
            self._heading = []

    def handle_endtag(self, tag):
        if tag in ('td', 'th') and self._cell is not None and self._row is not None:
            self._row.append(re.sub(r'\s+', ' ', ''.join(self._cell)).strip())
            self._cell = None
        elif tag == 'tr' and self._row is not None and self._stack:
            if any(self._row):
                self._stack[-1]['rows'].append(self._row)
            self._row = None
        elif tag == 'caption' and self._caption is not None and self._stack:
            self._stack[-1]['caption'] = re.sub(r'\s+', ' ', ''.join(self._caption)).strip()
            self._caption = None
        elif tag == 'table' and self._stack:
            self.tables.append(self._stack.pop())
        elif tag in ('h1', 'h2', 'h3', 'h4') and self._heading is not None:
            self.last_heading = re.sub(r'\s+', ' ', ''.join(self._heading)).strip()
            self._heading = None

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)
        if self._caption is not None:
            self._caption.append(data)
        if self._heading is not None:
            self._heading.append(data)


def _tables_from_html(html: str) -> List[dict]:
    g = _TableGrab()
    try:
        g.feed(html)
    except Exception as e:
        print(f'      table parse error: {type(e).__name__}: {e}')
    return g.tables


def _log_structure(page: str, html: str) -> None:
    """One compact look at how a page carries its chart data."""
    low = html.lower()
    counts = {t: low.count('<' + t) for t in ('svg', 'table', 'script', 'iframe', 'object', 'canvas')}
    base = re.search(r'<base[^>]+href="([^"]+)"', html, re.I)
    print(f'      structure: {counts} base={base.group(1) if base else None}')
    # The biggest inline <svg> is a chart, not a logo.
    svgs = [(m.start(), html.find('</svg>', m.start())) for m in re.finditer(r'<svg\b', html, re.I)]
    svgs = sorted([(e - b, b) for b, e in svgs if e > b], reverse=True)[:2]
    for size, b in svgs:
        chunk = re.sub(r'\s+', ' ', html[b:b + 700])
        texts = re.findall(r'<text[^>]*>([^<]{1,40})</text>', html[b:b + size])[:25]
        titles = re.findall(r'<title>([^<]{1,80})</title>', html[b:b + size])[:8]
        datas = re.findall(r'data-[a-z-]+="[^"]{0,60}"', html[b:b + size])[:8]
        print(f'      svg {size} B: {chunk[:500]}')
        print(f'        texts: {texts}')
        if titles: print(f'        titles: {titles}')
        if datas: print(f'        data-attrs: {datas}')
    scripts = [m.group(1) for m in re.finditer(r'<script[^>]*>(.*?)</script>', html, re.I | re.S)]
    scripts = sorted(scripts, key=len, reverse=True)[:2]
    for sc in scripts:
        head = re.sub(r'\s+', ' ', sc[:300])
        print(f'      script {len(sc)} B: {head}')


def _js_values(body: str) -> List[object]:
    """Items of a JS array literal body: quoted strings, numbers, null."""
    out: List[object] = []
    for m in re.finditer(r"""'((?:[^'\\]|\\.)*)'|"((?:[^"\\]|\\.)*)"|(-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)|\b(null|NaN|undefined)\b""", body):
        if m.group(1) is not None: out.append(m.group(1))
        elif m.group(2) is not None: out.append(m.group(2))
        elif m.group(3) is not None: out.append(float(m.group(3)))
        else: out.append(None)
    return out


def _balanced(text: str, start: int, open_ch: str, close_ch: str) -> int:
    """Index just past the bracket matching text[start] (which is open_ch)."""
    depth, i, q = 0, start, None
    while i < len(text):
        c = text[i]
        if q:
            if c == '\\':
                i += 2
                continue
            if c == q:
                q = None
        elif c in '\'"`':
            q = c
        elif c == open_ch:
            depth += 1
        elif c == close_ch:
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return len(text)


def _charts_from_scripts(html: str) -> List[dict]:
    """
    The chart pages draw with Chart.js and inline the complete data:

        const data_myChartId_870296 = { labels: ['01.01.2022', ...],
                                        datasets: [{ label: 'Norwegen', data: [...] }, ...] }

    Each such object becomes {id, title, labels, datasets}. The `_export`
    twin of a chart carries the same series; the longer of the two wins.
    """
    charts: Dict[str, dict] = {}
    for m in re.finditer(r'(?:const|var|let)\s+data_(myChartId_\w+?)(_export)?\s*=\s*\{', html):
        cid = m.group(1)
        obj_start = m.end() - 1
        obj = html[obj_start:_balanced(html, obj_start, '{', '}')]
        lm = re.search(r'labels\s*:\s*\[', obj)
        dm = re.search(r'datasets\s*:\s*\[', obj)
        if not lm or not dm:
            continue
        labels = _js_values(obj[lm.end():_balanced(obj, lm.end() - 1, '[', ']') - 1])
        ds_body = obj[dm.end() - 1:_balanced(obj, dm.end() - 1, '[', ']')]
        datasets = []
        i = 0
        while True:
            j = ds_body.find('{', i)
            if j < 0:
                break
            k = _balanced(ds_body, j, '{', '}')
            part = ds_body[j:k]
            i = k
            lab = re.search(r"""label\s*:\s*(['"])(.*?)\1""", part)
            dat = re.search(r'\bdata\s*:\s*\[', part)
            if not dat:
                continue
            vals = _js_values(part[dat.end():_balanced(part, dat.end() - 1, '[', ']') - 1])
            datasets.append({'label': lab.group(2) if lab else f'Reihe {len(datasets) + 1}',
                             'data': vals})
        if not datasets:
            continue
        # Title: the nearest heading before this chart's canvas.
        pos = html.find(f'id="{cid}"')
        pos = pos if pos >= 0 else m.start()
        heads = re.findall(r'<h[1-4][^>]*>(.*?)</h[1-4]>', html[max(0, pos - 20000):pos], re.S)
        title = re.sub(r'<[^>]+>|\s+', ' ', heads[-1]).strip() if heads else ''
        npts = sum(len(d['data']) for d in datasets)
        old = charts.get(cid)
        if not old or npts > sum(len(d['data']) for d in old['datasets']):
            charts[cid] = {'id': cid, 'title': title, 'labels': labels, 'datasets': datasets}
    return list(charts.values())


def _chart_to_csv(ch: dict) -> str:
    """Chart.js arrays -> the semicolon text parse_csv understands."""
    cols = [d['label'].replace(';', ',') for d in ch['datasets']]
    lines = ['Datum;' + ';'.join(cols)]
    for n, x in enumerate(ch['labels']):
        cells = []
        for d in ch['datasets']:
            v = d['data'][n] if n < len(d['data']) else None
            cells.append('' if v is None else (f'{v}' if isinstance(v, float) else str(v)))
        lines.append(f'{str(x).replace(";", ",")};' + ';'.join(cells))
    return '\n'.join(lines)


def _chart_pages(html: str, page: str) -> List[str]:
    """Links to other gas-supply chart pages (_svg/...html), resolved the way
    the browser does: this site sets <base href>, so "DE/..." is from root."""
    base = re.search(r'<base[^>]+href="([^"]+)"', html, re.I)
    root = base.group(1) if base else _ORIGIN + '/'
    if root.startswith('/'):
        root = _ORIGIN + root          # the site sets <base href="/">
    out = []
    for h in re.findall(r'href="([^"]+)"', html):
        h = h.replace('&amp;', '&').split('#')[0]
        if '_svg/' not in h or not re.search(r'\.html(\?|$)', h):
            continue
        if h.startswith('http'):
            u = h
        elif h.startswith('/'):
            u = _ORIGIN + h
        else:
            u = root.rstrip('/') + '/' + h
        u = u.split('?')[0].replace('http://', 'https://')
        if u not in out:
            out.append(u)
    return out


def _slug(u: str) -> str:
    m = re.search(r'_svg/([^/]+)/', u)
    name = m.group(1) if m else u.rsplit('/', 1)[-1].split('.')[0]
    return re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')


_GROUP_HINTS = [
    ('lng', 'Importe & Exporte'), ('import', 'Importe & Exporte'), ('export', 'Importe & Exporte'),
    ('foerder', 'Förderung & Preise'), ('preis', 'Förderung & Preise'),
    ('speicher', 'Speicher'), ('rlm', 'Industrie (RLM)'), ('industrie', 'Industrie (RLM)'),
    ('slp', 'Haushalte & Gewerbe (SLP)'), ('haushalt', 'Haushalte & Gewerbe (SLP)'),
    ('verbrauch', 'Verbrauch gesamt'), ('temp', 'Temperatur'),
]


def _group_for(slug: str) -> str:
    for hint, g in _GROUP_HINTS:
        if hint in slug:
            return g
    return 'Weitere'


def _unit_for(text: str) -> str:
    m = re.search(r'(GWh/(?:Tag|d)|TWh|EUR/MWh|€/MWh|°C|%-Punkte|Prozent|%)', text or '')
    return m.group(1).replace('€', 'EUR') if m else ''


def _freq_for(slug: str, x_kind: str) -> str:
    if 'woch' in slug: return 'weekly'
    if 'monat' in slug: return 'monthly'
    return 'daily' if x_kind == 'date' else ''


def _harvest_pages(prev: Dict[str, dict], out: Dict[str, dict], errors: Dict[str, str]) -> int:
    """Read every chart page; turn each data table into a dataset."""
    pages = [LANDING, BASE + 'Gasimporte/Gasimporte.html']
    seen = set(pages)
    found = 0
    for i, page in enumerate(pages):
        if i >= 40:
            break
        try:
            r = _get(page)
            if not r.ok:
                # start.html answered 404 once and 200 a few seconds later.
                time.sleep(2)
                r = _get(page)
            if not r.ok:
                print(f'    page {page.replace(_ORIGIN, "")}: HTTP {r.status_code}')
                continue
            html = r.text
        except Exception as e:
            print(f'    page {page.replace(_ORIGIN, "")}: {type(e).__name__}')
            continue
        tables = _tables_from_html(html)
        new = [u for u in _chart_pages(html, page) if u not in seen and u != page]
        print(f'    page {page.replace(_ORIGIN, "")}: {len(html)} B, {len(tables)} tables, '
              f'{len(new)} new chart pages')
        if i < 3:
            _log_structure(page, html)
            for u in new[:30]:
                print(f'      chart page: {u.replace(_ORIGIN, "")}')
        for u in new:
            seen.add(u)
            pages.append(u)
        slug = _slug(page) if '_svg/' in page else 'start'
        # Chart.js data inlined in the page scripts — where the numbers are.
        for ch in _charts_from_scripts(html):
            try:
                parsed = parse_csv(_chart_to_csv(ch), delimiter=';')
            except Exception as e:
                print(f'      chart {ch["id"]}: parse failed {type(e).__name__}: {e}')
                continue
            npts = sum(len(v) for v in parsed['series'].values())
            if npts < 5:
                print(f'      chart {ch["id"]}: only {npts} values, labels[:3]={ch["labels"][:3]}')
                continue
            key = f'{slug}_{ch["id"].replace("myChartId_", "c")}'
            title = ch['title'] or slug
            node = _merge(prev.get(key), parsed)
            node.update({'label': title[:90], 'group': _group_for(slug + ' ' + title.lower()),
                         'unit': _unit_for(title), 'freq': _freq_for(slug, node['x_kind']),
                         'url': page, 'ok': True, 'source_kind': 'chartjs'})
            out[key] = node
            found += 1
            xs = [x for v in node['series'].values() for x, _ in v]
            print(f'      chart {key}: "{title[:60]}" kind={node["x_kind"]} cols={node["columns"][:8]} '
                  f'pts={npts} x={min(xs)}..{max(xs)}')
        for n, t in enumerate(tables, start=1):
            rows = t['rows']
            if len(rows) < 3:
                continue
            text = '\n'.join(';'.join(c.replace(';', ',') for c in r_) for r_ in rows)
            try:
                # Cells hold German decimals ("1.234,5"), so never sniff a comma.
                parsed = parse_csv(text, delimiter=';')
            except Exception:
                continue
            npts = sum(len(v) for v in parsed['series'].values())
            if npts < 5:
                continue
            key = f'{slug}_t{n}'
            title = t['caption'] or t['title'] or slug
            node = _merge(prev.get(key), parsed)
            node.update({'label': title[:90], 'group': _group_for(slug + ' ' + title.lower()),
                         'unit': _unit_for(title + ' ' + ' '.join(rows[0])),
                         'freq': _freq_for(slug, node['x_kind']),
                         'url': page, 'ok': True, 'source_kind': 'html_table'})
            out[key] = node
            found += 1
            xs = [x for v in node['series'].values() for x, _ in v]
            print(f'      table {key}: "{title[:60]}" kind={node["x_kind"]} '
                  f'cols={len(node["columns"])} pts={npts} x={min(xs)}..{max(xs)} head={rows[0][:6]}')
    return found


def fetch() -> dict:
    prev = (store.read_json('bnetza_gas') or {}).get('data', {}).get('datasets', {})
    out: Dict[str, dict] = {}
    errors: Dict[str, str] = {}

    # 1. Tables on the chart pages — where the data actually is.
    n_tables = _harvest_pages(prev, out, errors)
    print(f'    {n_tables} data tables read from the chart pages')

    # 2. CSV files, in case the site links any (it did not in the first runs).
    seed_dirs = sorted({p.split('/')[0] for p, *_ in DATASETS.values()})
    discovered = _discover(seed_dirs) if not n_tables else {}
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

    if n_tables:
        # The guessed CSV names all answered 404; with tables available they
        # are not worth 38 requests per run.
        jobs = [j for j in jobs if j[0] in prev and prev[j[0]].get('series')]
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

    # A table that did not turn up this run keeps its stored history.
    for k, node in prev.items():
        if k not in out and node.get('series'):
            kept = dict(node)
            kept['ok'] = False
            kept['error'] = 'in diesem Lauf nicht gefunden — gespeicherter Stand'
            out[k] = kept

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
