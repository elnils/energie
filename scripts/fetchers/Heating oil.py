"""
Tecson — German heating oil reference price + oil-market notations.

Scraping target: https://www.tecson.de/de/heizoelpreise.html
The page is rendered server-side (Contao CMS), no JS dependency.

Strategy: locate distinctive German phrases in the HTML and walk near them
to grab the numeric values. We use BOTH BeautifulSoup (when available) and a
regex fallback so the workflow doesn't break if BS4 install fails.

Daily averages and quarterly aggregates are extracted into stable fields.
On selector miss, we raise — the wrapper preserves last good values.

Updated: every weekday morning before 10 CET.
"""
import re
from typing import Dict, Optional

from core import http, validators

URL = 'https://www.tecson.de/de/heizoelpreise.html'


def _parse_de_decimal(s: str) -> Optional[float]:
    """Parse German number like '1,317' or '131,7' into float."""
    if not s:
        return None
    s = s.strip().replace('.', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return None


def _extract_reference_price(html: str) -> Optional[float]:
    """
    Find the 'Referenzpreis aktuell' table row.
    The page renders something like:
        | Referenzpreis aktuell ... | am 7. Mai Ø 1,317 €/l |
    We grab the EUR/Liter number on that line.
    """
    # Look for "Ø <num> €/l" — Ø is U+00D8 in their HTML
    m = re.search(r'Ø\s*([0-9]+[,.]?[0-9]*)\s*€\s*/\s*l', html)
    if m:
        return _parse_de_decimal(m.group(1))
    # Backup: cent/liter mention in headline
    m = re.search(r'auf\s+([0-9]+[,.][0-9]+)\s*Cent/Liter', html)
    if m:
        cents = _parse_de_decimal(m.group(1))
        return round(cents / 100.0, 4) if cents is not None else None
    return None


def _extract_change_table(html: str) -> Dict[str, Optional[float]]:
    """
    Pull the 'Preisveränderung beim Heizöl' table:
        n,n% [günstiger|teurer] als gestern    mit X,XXX €/l
        n,n% [günstiger|teurer] als vor 1 Woche  mit X,XXX €/l
        ...
    """
    results: Dict[str, Optional[float]] = {
        'yesterday_eur_l': None, 'yesterday_pct': None,
        'week_eur_l': None, 'week_pct': None,
        'month_eur_l': None, 'month_pct': None,
        'year_eur_l': None, 'year_pct': None,
    }
    targets = [
        ('yesterday', r'als\s+gestern'),
        ('week',      r'als\s+vor\s+1\s+Woche'),
        ('month',     r'als\s+vor\s+1\s+Monat'),
        ('year',      r'als\s+vor\s+1\s+Jahr'),
    ]
    for key, anchor in targets:
        # pattern: <pct>% (günstiger|teurer) als <anchor> ... <price> €/l
        pat = (r'([0-9]+[,.][0-9]+)\s*%\s*(g[üu]nstiger|teurer)\s+'
               + anchor
               + r'.{0,200}?([0-9]+[,.][0-9]+)\s*€\s*/\s*l')
        m = re.search(pat, html, re.DOTALL | re.IGNORECASE)
        if m:
            pct = _parse_de_decimal(m.group(1))
            direction = m.group(2).lower()
            price = _parse_de_decimal(m.group(3))
            if pct is not None and direction.startswith(('g', 'gü')):
                pct = -pct  # cheaper means lower
            results[f'{key}_eur_l'] = price
            results[f'{key}_pct'] = pct
    return results


def _extract_oil_notations(html: str) -> Dict[str, Optional[float]]:
    """
    Grab the 'Ölnotierungen am ...morgen' table:
        Brent Rohöl    | 101,5 $/bbl (-6,3)
        WTI Crude Oil  | 95,1  $/bbl (-5,0)
        Opec-Basket    | 112,3 $/bbl (-6,0)
        Gasöl          | 994 €/Tonne (-85)
    """
    out = {
        'brent_usd_bbl': None, 'brent_change': None,
        'wti_usd_bbl': None,   'wti_change': None,
        'opec_usd_bbl': None,  'opec_change': None,
        'gasoil_eur_t': None,  'gasoil_change': None,
    }
    pairs = [
        ('brent', r'Brent\s*Roh[öo]l',           r'\$\s*/\s*bbl', 'usd_bbl'),
        ('wti',   r'WTI\s*Crude\s*Oil',          r'\$\s*/\s*bbl', 'usd_bbl'),
        ('opec',  r'Opec[\s\-]*Basket',          r'\$\s*/\s*bbl', 'usd_bbl'),
        ('gasoil', r'Gas[öo]l',                   r'€\s*/\s*Tonne', 'eur_t'),
    ]
    for key, name_pat, unit_pat, unit_suffix in pairs:
        # Allow filler (parenthetical, table cell separators, etc) between
        # the label and the value. Up to 200 chars of slack. Decimals optional
        # so integer values like '994 €/Tonne' are matched.
        pat = (name_pat + r'[^0-9$€]{0,200}?([0-9]+(?:[,.][0-9]+)?)\s*' + unit_pat
               + r'(?:\s*\(([+\-−][0-9,.\s]+)\))?')
        m = re.search(pat, html, re.DOTALL)
        if m:
            out[f'{key}_{unit_suffix}'] = _parse_de_decimal(m.group(1))
            change_raw = m.group(2)
            if change_raw:
                # Replace minus-variants and strip
                change_clean = change_raw.replace('−', '-').replace(' ', '')
                out[f'{key}_change'] = _parse_de_decimal(change_clean)
    return out


def _extract_quarterly(html: str) -> Dict[str, Optional[float]]:
    """
    Grab quarterly/yearly averages from the 'Heizöl-Durchschnittspreise' table.
    Format: '| II. Quartal '26 | Ø 144,4 EUR |' (per 100 L)
    Returns {'q2_2026_eur_100l': 144.4, 'y2025_eur_100l': 96.1, ...}
    """
    out: Dict[str, Optional[float]] = {}
    # Apostrophe variants the page may use: ASCII ', typographic ' ', backtick `
    APOS = r"['\u2018\u2019\u201A\u201B`]"
    # Quarterly: roman numeral + year
    for m in re.finditer(
            rf"(I{{1,3}}V?|IV)\.\s*Quartal\s*{APOS}?(\d{{2}})\s*(?:\||</td>|\s)\s*(?:<[^>]+>\s*)*Ø\s*([0-9]+[,.][0-9]+)\s*EUR",
            html):
        roman, yy, val = m.group(1), m.group(2), m.group(3)
        roman_to_q = {'I': 1, 'II': 2, 'III': 3, 'IV': 4}
        q = roman_to_q.get(roman)
        if q:
            out[f'q{q}_20{yy}_eur_100l'] = _parse_de_decimal(val)
    # Yearly: 'gesamt 2025 | Ø 96,1 EUR'
    for m in re.finditer(
            r"gesamt\s+(\d{4})\s*(?:\||</td>|\s)\s*(?:<[^>]+>\s*)*Ø\s*([0-9]+[,.][0-9]+)\s*EUR",
            html):
        year, val = m.group(1), m.group(2)
        out[f'y{year}_eur_100l'] = _parse_de_decimal(val)
    return out


def fetch() -> dict:
    s = http.get_session()
    r = s.get(URL, timeout=25, headers={
        'Accept': 'text/html,application/xhtml+xml',
    })
    r.raise_for_status()
    html = r.text

    ref_price = _extract_reference_price(html)
    if ref_price is None:
        raise RuntimeError('Tecson: could not extract reference price (selector broken?)')

    if not validators.in_range('heating_oil_eur_l', ref_price):
        raise RuntimeError(f'Tecson: reference price {ref_price} out of range — selector likely wrong')

    changes = _extract_change_table(html)
    notations = _extract_oil_notations(html)
    quarterly = _extract_quarterly(html)

    print(f'    tecson: ref={ref_price} EUR/L, brent={notations.get("brent_usd_bbl")}, '
          f'wti={notations.get("wti_usd_bbl")}, gasoil={notations.get("gasoil_eur_t")}')

    return {
        'data': {
            'reference_price_eur_l': ref_price,
            'changes': changes,
            'oil_notations': notations,
            'quarterly_avg_eur_100l': quarterly,
        },
        'meta': {
            'source': 'TECSON Erhebung (https://www.tecson.de)',
            'license': 'attribution required, no automated commercial use',
            'units': 'EUR per liter for heating oil; per 100L for quarterly averages',
            'price_basis': '3000 L delivery, sulphur-poor, incl. 19% VAT',
        },
    }
