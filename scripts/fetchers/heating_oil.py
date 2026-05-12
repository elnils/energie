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

Changelog:
  - Fixed duplicate `return out` in _extract_quarterly() (was dead code, now removed)
  - Added _extract_annual_trend(): year-on-year EUR/L annual averages
"""
import re
from typing import Dict, List, Optional

from core import http, validators, history

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
    Find the current Tecson reference price for heating oil (EUR/L).

    Tecson varies their HTML structure between releases. We try multiple
    patterns in priority order. Each captures either an EUR/L or Cent/L
    value and converts to EUR/L.
    """
    patterns = [
        # Format 1 (older): "am 7. Mai Ø 1,317 €/l"
        (r'Ø\s*([0-9]+[,.]?[0-9]*)\s*€\s*/\s*l',     'eur_l'),
        # Format 2 (older): "auf 131,7 Cent/Liter"
        (r'auf\s+([0-9]+[,.][0-9]+)\s*Cent/Liter',    'cent_l'),
        # Format 3 (current May 2026): "bei 134,7 Cent/Liter"
        (r'bei\s+([0-9]+[,.][0-9]+)\s*Cent/Liter',    'cent_l'),
        # Format 4 (also current): "HEL Ø-Preis ... 134,7 Cent/Liter"
        (r'HEL\s*Ø[\-\s]*Preis[^0-9]*([0-9]+[,.][0-9]+)\s*Cent/Liter', 'cent_l'),
        # Format 5: any "Ø ... Cent/Liter" structure
        (r'Ø[\-\s]*Preis[^0-9]*([0-9]+[,.][0-9]+)\s*Cent\s*/\s*Liter', 'cent_l'),
        # Format 6 (last resort): standalone "<num> Cent/Liter" preceded by news header
        (r'Heizöl[^0-9]{0,50}([0-9]{2,3}[,.][0-9])\s*Cent/Liter', 'cent_l'),
    ]
    for pattern, unit in patterns:
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            val = _parse_de_decimal(m.group(1))
            if val is None:
                continue
            if unit == 'cent_l':
                val = round(val / 100.0, 4)
            # Sanity: heating oil EUR/L should be 0.3 - 5.0
            if 0.3 < val < 5.0:
                return val
    return None


def _extract_change_table(html: str) -> Dict[str, Optional[float]]:
    """
    Pull the 'Preisveränderung beim Heizöl' table.
    Tecson varies the wording: 'als gestern' / 'als am Donnerstag' (weekday name) /
    'als am Vortag'. Match all of them.
    Format: 'X,X% [günstiger|teurer] als <anchor>  mit Y,YYY €/l'
    """
    results: Dict[str, Optional[float]] = {
        'yesterday_eur_l': None, 'yesterday_pct': None,
        'week_eur_l': None, 'week_pct': None,
        'month_eur_l': None, 'month_pct': None,
        'year_eur_l': None, 'year_pct': None,
    }
    weekdays = r'(?:Montag|Dienstag|Mittwoch|Donnerstag|Freitag|Samstag|Sonntag|Vortag|gestern)'
    targets = [
        ('yesterday', rf'als\s+(?:am\s+)?{weekdays}'),
        ('week',      r'als\s+vor\s+1?\s*Woche'),
        ('month',     r'als\s+vor\s+1?\s*Monat'),
        ('year',      r'als\s+vor\s+1?\s*Jahr'),
    ]
    for key, anchor in targets:
        pat = (r'([0-9]+[,.][0-9]+)\s*%\s*(g[üu]nstiger|teurer)\s+'
               + anchor
               + r'.{0,250}?([0-9]+[,.][0-9]+)\s*€\s*/\s*l')
        m = re.search(pat, html, re.DOTALL | re.IGNORECASE)
        if m:
            pct = _parse_de_decimal(m.group(1))
            direction = m.group(2).lower()
            price = _parse_de_decimal(m.group(3))
            if pct is not None and direction.startswith(('g', 'gü')):
                pct = -pct
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
        pat = (name_pat + r'[^0-9$€]{0,200}?([0-9]+(?:[,.][0-9]+)?)\s*' + unit_pat
               + r'(?:\s*\(([+\-−][0-9,.\s]+)\))?')
        m = re.search(pat, html, re.DOTALL)
        if m:
            out[f'{key}_{unit_suffix}'] = _parse_de_decimal(m.group(1))
            change_raw = m.group(2)
            if change_raw:
                change_clean = change_raw.replace('−', '-').replace(' ', '')
                out[f'{key}_change'] = _parse_de_decimal(change_clean)
    return out


def _extract_quarterly(html: str) -> Dict[str, Optional[float]]:
    """
    Grab quarterly/yearly averages from the 'Heizöl-Durchschnittspreise' table.
    Format: '| II. Quartal '26 | Ø 144,4 EUR |' (per 100 L)
    """
    out: Dict[str, Optional[float]] = {}
    APOS = r"['\u2018\u2019\u201A\u201B`]"
    for m in re.finditer(
            rf"(I{{1,3}}V?|IV)\.\s*Quartal\s*{APOS}?(\d{{2}})\s*[\s\|<>/td]*\s*Ø\s*([0-9]+[,.][0-9]+)\s*EUR",
            html):
        roman, yy, val = m.group(1), m.group(2), m.group(3)
        roman_to_q = {'I': 1, 'II': 2, 'III': 3, 'IV': 4}
        q = roman_to_q.get(roman)
        if q:
            out[f'q{q}_20{yy}_eur_100l'] = _parse_de_decimal(val)
    for m in re.finditer(
            r"gesamt\s+(\d{4})\s*[\s\|<>/td]*\s*Ø\s*([0-9]+[,.][0-9]+)\s*EUR",
            html):
        year, val = m.group(1), m.group(2)
        out[f'y{year}_eur_100l'] = _parse_de_decimal(val)
    return out   # ← single return (duplicate removed)


def _extract_annual_trend(html: str) -> List[Dict]:
    """
    Extract the multi-year annual average table if present.
    Looks for patterns like: 2020  Ø 50,1 EUR  (per 100L)
    Returns list of {year: int, eur_100l: float} sorted ascending.
    This complements the quarterly data with a longer historical view.
    """
    results = []
    seen_years = set()
    for m in re.finditer(
            r'\b(20\d{2})\b[^0-9€]*Ø\s*([0-9]+[,.][0-9]+)\s*EUR',
            html):
        year_str, val_str = m.group(1), m.group(2)
        year = int(year_str)
        if year in seen_years or year < 2010 or year > 2030:
            continue
        val = _parse_de_decimal(val_str)
        if val and 20.0 < val < 500.0:   # sanity: EUR/100L range
            results.append({'year': year, 'eur_100l': val})
            seen_years.add(year)
    results.sort(key=lambda x: x['year'])
    return results


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
    annual_trend = _extract_annual_trend(html)

    print(f'    tecson: ref={ref_price} EUR/L, brent={notations.get("brent_usd_bbl")}, '
          f'wti={notations.get("wti_usd_bbl")}, gasoil={notations.get("gasoil_eur_t")}, '
          f'quarterly={len(quarterly)} entries, annual={len(annual_trend)} years')

    # History: append daily snapshot for long-term trend chart
    history.record_history('heating_oil', {
        'ref_eur_l':     ref_price,
        'brent_usd_bbl': notations.get('brent_usd_bbl'),
        'wti_usd_bbl':   notations.get('wti_usd_bbl'),
        'gasoil_eur_t':  notations.get('gasoil_eur_t'),
    })

    return {
        'data': {
            'reference_price_eur_l': ref_price,
            'changes': changes,
            'oil_notations': notations,
            'quarterly_avg_eur_100l': quarterly,
            'annual_trend': annual_trend,
        },
        'meta': {
            'source': 'TECSON Erhebung (https://www.tecson.de)',
            'license': 'attribution required, no automated commercial use',
            'units': 'EUR per liter for heating oil; per 100L for quarterly/annual averages',
            'price_basis': '3000 L delivery, sulphur-poor, incl. 19% VAT',
        },
    }
