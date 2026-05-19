"""
Tecson — German heating oil reference price + oil-market notations.
Destatis — supplemental historical heating oil price index (tables from
           communicated 2026-05-13).

Scraping target: https://www.tecson.de/de/heizoelpreise.html
The page is rendered server-side (Contao CMS), no JS dependency.

Strategy:
  1. Normalize HTML via html.unescape() BEFORE all regex work so that
     entity variants (Roh&ouml;l, &Oslash;, &#216;) are transparent.
  2. Run multiple patterns per field in priority order.
  3. Supplement Tecson-only snapshots with Destatis monthly VPI for
     heating oil (tables 43531-0005 and 61241-0001) to give the
     dashboard a back-filled price index — called gracefully and skipped
     on timeout without failing the whole job.

Bug fixes vs previous version:
  - BUG: Brent regex used German label 'Brent Rohöl' which Tecson changed
    to the English 'Brent Crude Oil' (same as WTI). Fixed by trying both.
  - BUG: Gasoil regex used 'Gas[öo]l' but HTML entity 'Gas&ouml;l' was
    not being unescaped first. Fixed by calling html.unescape() upfront.
  - BUG: Quarterly/annual regex required literal 'Ø' which appears as
    '&Oslash;' or '&#216;' in Contao's HTML. Fixed by pre-unescaping and
    adding DOTALL + wider character class between tokens.
  - BUG: Duplicate 'return out' on the last two lines of _extract_quarterly
    (dead code, removed).

Updated: every weekday morning before 10 CET.
"""
import html as html_lib
import os
import re
from typing import Dict, List, Optional

from core import http, validators, history

URL = 'https://www.tecson.de/de/heizoelpreise.html'

# Destatis tables added 2026-05-13.
# 43531-0005 = Verbraucherpreise Heizöl, monatlich (EUR/100L nominal)
# 61241-0001 = Erzeugerpreisindex Mineralölerzeugnisse, monatlich (Index 2015=100)
DESTATIS_TABLE_HEIZOIL_CONSUMER = '43531-0005'
DESTATIS_TABLE_HEIZOIL_INDEX    = '61241-0001'


# ──────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ──────────────────────────────────────────────────────────────────────

def _normalize(raw_html: str) -> str:
    """
    Decode all HTML entities (&ouml; → ö, &Oslash; → Ø, &#216; → Ø …)
    so that every downstream regex works on plain Unicode.
    This is the single most important pre-processing step: Contao CMS
    frequently entity-encodes special characters (umlauts, Ø) inside
    table cells, making naive regex patterns fail silently.
    """
    return html_lib.unescape(raw_html)


def _parse_de_decimal(s: str) -> Optional[float]:
    """Parse German-locale number like '1,317' or '131,7' into float."""
    if not s:
        return None
    s = s.strip().replace('.', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return None


# ──────────────────────────────────────────────────────────────────────
# TECSON EXTRACTORS  (all receive pre-normalized plain-Unicode HTML)
# ──────────────────────────────────────────────────────────────────────

def _extract_reference_price(html: str) -> Optional[float]:
    """
    Find the current Tecson reference price for heating oil.
    Tries EUR/L patterns first, then Cent/L (converts to EUR/L).
    Sanity bound: 0.30 – 5.00 EUR/L.
    """
    patterns = [
        # Explicit EUR/L with Ø average marker
        (r'Ø\s*([0-9]+[,.]?[0-9]*)\s*€\s*/\s*l',                        'eur_l'),
        # Cent/Liter variants — preposition varies by Tecson version
        (r'(?:auf|bei|von)\s+([0-9]+[,.][0-9]+)\s*Cent/Liter',           'cent_l'),
        # "HEL Ø-Preis … 134,7 Cent/Liter"
        (r'HEL\s*Ø[\-\s]*Preis[^0-9]{0,80}([0-9]+[,.][0-9]+)\s*Cent/Liter', 'cent_l'),
        # Any "Ø … Cent/Liter"
        (r'Ø[\-\s]*Preis[^0-9]{0,80}([0-9]+[,.][0-9]+)\s*Cent\s*/\s*Liter', 'cent_l'),
        # Fallback: "Heizöl … <num> Cent/Liter" within 80 chars
        (r'Heiz[öo]l[^0-9]{0,80}([0-9]{2,3}[,.][0-9])\s*Cent/Liter',    'cent_l'),
    ]
    for pattern, unit in patterns:
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            val = _parse_de_decimal(m.group(1))
            if val is None:
                continue
            if unit == 'cent_l':
                val = round(val / 100.0, 4)
            if 0.30 < val < 5.00:
                return val
    return None


def _extract_change_table(html: str) -> Dict[str, Optional[float]]:
    """
    Pull the 'Preisveränderung beim Heizöl' table.
    Format: 'X,X% [günstiger|teurer] als <anchor>  mit Y,YYY €/l'
    Weekday names and 'Vortag'/'gestern' are all matched.
    """
    results: Dict[str, Optional[float]] = {
        'yesterday_eur_l': None, 'yesterday_pct': None,
        'week_eur_l':      None, 'week_pct':      None,
        'month_eur_l':     None, 'month_pct':     None,
        'year_eur_l':      None, 'year_pct':      None,
    }
    weekdays = (r'(?:Montag|Dienstag|Mittwoch|Donnerstag|Freitag'
                r'|Samstag|Sonntag|Vortag|gestern)')
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
            pct   = _parse_de_decimal(m.group(1))
            direc = m.group(2).lower()
            price = _parse_de_decimal(m.group(3))
            if pct is not None and direc.startswith(('g', 'gü')):
                pct = -pct
            results[f'{key}_eur_l'] = price
            results[f'{key}_pct']   = pct
    return results


def _extract_oil_notations(html: str) -> Dict[str, Optional[float]]:
    """
    Grab the 'Ölnotierungen' table.

    FIX (2026-05-13): Tecson changed 'Brent Rohöl' → 'Brent Crude Oil'.
    We now try BOTH forms. html.unescape() in _normalize() already handles
    'Gas&ouml;l' → 'Gasöl', so the gasoil pattern now also works correctly.

    Expected values:
        Brent Crude Oil / Rohöl  → $/bbl
        WTI Crude Oil            → $/bbl
        Opec-Basket              → $/bbl
        Gasöl / Gasoil           → €/Tonne
    """
    out = {
        'brent_usd_bbl': None, 'brent_change': None,
        'wti_usd_bbl':   None, 'wti_change':   None,
        'opec_usd_bbl':  None, 'opec_change':  None,
        'gasoil_eur_t':  None, 'gasoil_change': None,
    }

    # Each entry: (output_key, [list of name patterns to try], unit_pattern, unit_suffix)
    pairs = [
        # Brent: try English first (current), German as fallback
        ('brent',  [r'Brent\s*Crude\s*Oil', r'Brent\s*Roh[öo]l', r'Brent'],
                   r'\$\s*/\s*bbl', 'usd_bbl'),
        ('wti',    [r'WTI\s*Crude\s*Oil', r'West\s*Texas'],
                   r'\$\s*/\s*bbl', 'usd_bbl'),
        ('opec',   [r'Opec[\s\-]*Basket', r'OPEC[\s\-]*Basket'],
                   r'\$\s*/\s*bbl', 'usd_bbl'),
        # Gasoil: after html.unescape() 'Gas&ouml;l' becomes 'Gasöl', both match
        ('gasoil', [r'Gas[öo]l', r'Gasoil', r'Gas-?oil'],
                   r'€\s*/\s*Tonne', 'eur_t'),
    ]

    for key, name_pats, unit_pat, unit_suffix in pairs:
        for name_pat in name_pats:
            pat = (name_pat
                   + r'[^0-9$€]{0,200}?([0-9]+(?:[,.][0-9]+)?)\s*'
                   + unit_pat
                   + r'(?:\s*\(([+\-−][0-9,.\s]+)\))?')
            m = re.search(pat, html, re.DOTALL | re.IGNORECASE)
            if m:
                out[f'{key}_{unit_suffix}'] = _parse_de_decimal(m.group(1))
                change_raw = m.group(2)
                if change_raw:
                    change_clean = change_raw.replace('−', '-').replace(' ', '')
                    out[f'{key}_change'] = _parse_de_decimal(change_clean)
                break   # first matching pattern wins; skip remaining for this commodity

    return out


def _extract_quarterly(html: str) -> Dict[str, Optional[float]]:
    """
    Grab quarterly and yearly averages from the 'Heizöl-Durchschnittspreise'
    table (EUR per 100 L, 3000 L delivery).

    FIX (2026-05-13): The Ø character (U+00D8) appears in Contao HTML as
    '&Oslash;' or '&#216;'. _normalize() decodes entities first so the Ø
    literal now matches. Additionally, the character class between the year
    token and the Ø now accepts any non-digit run (was too restrictive).

    Format examples (after unescape):
        II. Quartal '26   Ø 144,1 EUR       ← plain text / markdown
        | II. Quartal '26 | Ø 144,4 EUR |   ← pipe-table
        <td>II. Quartal '26</td><td>Ø 144,4 EUR</td>  ← HTML table
    """
    out: Dict[str, Optional[float]] = {}

    APOS = r"['\u2018\u2019\u201A\u201B`]"
    FILLER = r'[^0-9\u00d8]{0,120}'   # anything that is not a digit or Ø

    # Quarterly averages
    for m in re.finditer(
            rf"(I{{1,3}}V?|IV)\.\s*Quartal\s*{APOS}?(\d{{2}})"
            rf"{FILLER}[Ø\u00d8]\s*([0-9]+[,.][0-9]+)\s*EUR",
            html, re.IGNORECASE | re.DOTALL):
        roman, yy, val = m.group(1).upper(), m.group(2), m.group(3)
        roman_to_q = {'I': 1, 'II': 2, 'III': 3, 'IV': 4}
        q = roman_to_q.get(roman)
        if q:
            out[f'q{q}_20{yy}_eur_100l'] = _parse_de_decimal(val)

    # Yearly averages: "gesamt 2025 … Ø 96,1 EUR"
    for m in re.finditer(
            rf"gesamt\s+(\d{{4}}){FILLER}[Ø\u00d8]\s*([0-9]+[,.][0-9]+)\s*EUR",
            html, re.IGNORECASE | re.DOTALL):
        year, val = m.group(1), m.group(2)
        out[f'y{year}_eur_100l'] = _parse_de_decimal(val)

    return out   # single return — duplicate removed


# ──────────────────────────────────────────────────────────────────────
# DESTATIS SUPPLEMENT  (graceful — never raises, never breaks the job)
# ──────────────────────────────────────────────────────────────────────

def _fetch_destatis_supplement() -> Dict[str, List[dict]]:
    """
    Pull historical heating oil data from Destatis Genesis-Online.

    Tables added 2026-05-13:
      43531-0005  Verbraucherpreise Heizöl, monatlich, EUR/100L (nominal price)
      61241-0001  Erzeugerpreisindex Mineralölerzeugnisse (Index 2015=100)

    Returns a dict with keys 'consumer_eur_100l' and 'producer_index', each
    being a sorted list of {'period': 'YYYY-MM', 'v': float} dicts.
    Returns empty lists on any error (timeout, auth failure, parse error).

    Auth is read from the same env vars as destatis_vpi.py:
      DESTATIS_USERNAME + DESTATIS_PASSWORD  or  DESTATIS_API_TOKEN
    """
    result: Dict[str, List[dict]] = {
        'consumer_eur_100l': [],
        'producer_index':    [],
    }

    # Skip silently if no credentials are configured — avoids noise in logs
    # when the GitHub Actions secret is not set for the heating-oil job.
    user  = os.environ.get('DESTATIS_USERNAME', '').strip()
    pwd   = os.environ.get('DESTATIS_PASSWORD', '')
    token = os.environ.get('DESTATIS_API_TOKEN', '').strip()
    if not (user and pwd) and not token:
        print('    destatis supplement: no credentials configured, skipping')
        return result

    try:
        # Import the shared Destatis helpers from destatis_vpi rather than
        # duplicating the auth/tablefile machinery.
        from destatis_vpi import (
            _Credentials, _check_login, _post_auto,
            _fetch_tablefile, _parse_ffcsv,
        )
    except ImportError as e:
        print(f'    destatis supplement: import failed ({e}), skipping')
        return result

    try:
        creds     = _Credentials.from_env()
        auth_mode = _check_login(creds)
    except Exception as e:
        print(f'    destatis supplement: auth failed ({e}), skipping')
        return result

    DE_MONTHS = {
        'januar': 1, 'februar': 2, 'märz': 3, 'maerz': 3, 'april': 4,
        'mai': 5, 'juni': 6, 'juli': 7, 'august': 8, 'september': 9,
        'oktober': 10, 'november': 11, 'dezember': 12,
    }

    def _rows_to_series(rows: List[dict]) -> List[dict]:
        pts: List[dict] = []
        for r in rows:
            year  = (r.get('time') or '').strip()
            mlbl  = (r.get('time_label') or '').strip().lower()
            value = r.get('value')
            if not year or value is None:
                continue
            # Convert German month label to zero-padded integer
            m_int = DE_MONTHS.get(mlbl)
            period = f'{year}-{m_int:02d}' if m_int else year
            try:
                pts.append({'period': period, 'v': float(str(value).replace(',', '.'))})
            except (ValueError, TypeError):
                continue
        pts.sort(key=lambda x: x['period'])
        return pts

    # Table 43531-0005: consumer price heating oil, monthly
    try:
        rows = _fetch_tablefile(
            creds, DESTATIS_TABLE_HEIZOIL_CONSUMER,
            startyear=2019, mode=auth_mode,
        )
        result['consumer_eur_100l'] = _rows_to_series(rows)
        print(f'    destatis {DESTATIS_TABLE_HEIZOIL_CONSUMER}: '
              f'{len(result["consumer_eur_100l"])} pts (Heizöl EUR/100L)')
    except Exception as e:
        print(f'    destatis {DESTATIS_TABLE_HEIZOIL_CONSUMER}: {e}')

    # Table 61241-0001: producer price index mineral oil products
    try:
        rows = _fetch_tablefile(
            creds, DESTATIS_TABLE_HEIZOIL_INDEX,
            startyear=2019, mode=auth_mode,
        )
        result['producer_index'] = _rows_to_series(rows)
        print(f'    destatis {DESTATIS_TABLE_HEIZOIL_INDEX}: '
              f'{len(result["producer_index"])} pts (Erzeugerpreisindex)')
    except Exception as e:
        print(f'    destatis {DESTATIS_TABLE_HEIZOIL_INDEX}: {e}')

    return result


# ──────────────────────────────────────────────────────────────────────
# PUBLIC ENTRY POINT
# ──────────────────────────────────────────────────────────────────────

def fetch() -> dict:
    s = http.get_session()
    r = s.get(URL, timeout=25, headers={'Accept': 'text/html,application/xhtml+xml'})
    r.raise_for_status()

    # CRITICAL: normalize HTML entities before ANY regex work.
    # This single call fixes Brent, Gasöl and Ø matching simultaneously.
    html = _normalize(r.text)

    ref_price = _extract_reference_price(html)
    if ref_price is None:
        raise RuntimeError(
            'Tecson: could not extract reference price — '
            'selector broken or page structure changed. '
            'Check the six patterns in _extract_reference_price().'
        )

    if not validators.in_range('heating_oil_eur_l', ref_price):
        raise RuntimeError(
            f'Tecson: reference price {ref_price:.4f} EUR/L is outside the '
            f'allowed range — parser is likely matching the wrong number.'
        )

    changes   = _extract_change_table(html)
    notations = _extract_oil_notations(html)
    quarterly = _extract_quarterly(html)

    q_total   = len([k for k in quarterly if k.startswith('q')])
    yr_count  = len([k for k in quarterly if k.startswith('y')])


    print(
        f'    tecson: ref={ref_price} EUR/L'
        f', brent={notations.get("brent_usd_bbl")}'
        f', wti={notations.get("wti_usd_bbl")}'
        f', gasoil={notations.get("gasoil_eur_t")}'
        f', quarterly={q_total} entries'
        f', annual={yr_count} years'
    )

    # History: always write the ref price; Nones in optional fields are fine.
    history.record_history('heating_oil', {
        'ref_eur_l':     ref_price,
        'brent_usd_bbl': notations.get('brent_usd_bbl'),
        'wti_usd_bbl':   notations.get('wti_usd_bbl'),
        'gasoil_eur_t':  notations.get('gasoil_eur_t'),
    })

    # Destatis supplement — called after Tecson so a Destatis timeout
    # never prevents the primary Tecson data from being written.
    destatis = _fetch_destatis_supplement()

    return {
        'data': {
            'reference_price_eur_l':  ref_price,
            'changes':                changes,
            'oil_notations':          notations,
            'quarterly_avg_eur_100l': quarterly,
            'destatis_monthly':       destatis,
        },
        'meta': {
            'source':       'TECSON Erhebung (https://www.tecson.de)',
            'source_supplement': (
                'Destatis Genesis-Online: '
                f'{DESTATIS_TABLE_HEIZOIL_CONSUMER} (consumer EUR/100L), '
                f'{DESTATIS_TABLE_HEIZOIL_INDEX} (producer index 2015=100) '
                '— added 2026-05-13'
            ),
            'license':      'Tecson: attribution required, no automated commercial use. '
                            'Destatis: Datenlizenz Deutschland Namensnennung 2.0.',
            'units':        'EUR per liter for heating oil; per 100L for quarterly averages',
            'price_basis':  '3000 L delivery, sulphur-poor, incl. 19% VAT',
        },
    }
