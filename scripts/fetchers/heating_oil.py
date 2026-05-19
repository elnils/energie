"""
Tecson — German heating oil reference price + oil-market notations.
Destatis — supplemental historical heating oil price index.

v5.3 fixes:
  - q_count one-liner used `iter([list], '')` which is the two-argument
    iter(callable, sentinel) form; lists aren't callable. Removed the
    redundant variable, kept only q_total/yr_count.
  - destatis supplement used `from destatis_vpi import ...` (absolute),
    which fails because the module lives at `fetchers.destatis_vpi`.
    Fixed to a relative `from . import destatis_vpi as dv`.
"""
import html as html_lib
import os
import re
from typing import Dict, List, Optional

from core import http, validators, history

URL = 'https://www.tecson.de/de/heizoelpreise.html'

DESTATIS_TABLE_HEIZOIL_CONSUMER = '43531-0005'
DESTATIS_TABLE_HEIZOIL_INDEX    = '61241-0001'


def _normalize(raw_html: str) -> str:
    return html_lib.unescape(raw_html)


def _parse_de_decimal(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.strip().replace('.', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return None


def _extract_reference_price(html: str) -> Optional[float]:
    patterns = [
        (r'Ø\s*([0-9]+[,.]?[0-9]*)\s*€\s*/\s*l',                        'eur_l'),
        (r'(?:auf|bei|von)\s+([0-9]+[,.][0-9]+)\s*Cent/Liter',           'cent_l'),
        (r'HEL\s*Ø[\-\s]*Preis[^0-9]{0,80}([0-9]+[,.][0-9]+)\s*Cent/Liter', 'cent_l'),
        (r'Ø[\-\s]*Preis[^0-9]{0,80}([0-9]+[,.][0-9]+)\s*Cent\s*/\s*Liter', 'cent_l'),
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
    out = {
        'brent_usd_bbl': None, 'brent_change': None,
        'wti_usd_bbl':   None, 'wti_change':   None,
        'opec_usd_bbl':  None, 'opec_change':  None,
        'gasoil_eur_t':  None, 'gasoil_change': None,
    }
    pairs = [
        ('brent',  [r'Brent\s*Crude\s*Oil', r'Brent\s*Roh[öo]l', r'Brent'],
                   r'\$\s*/\s*bbl', 'usd_bbl'),
        ('wti',    [r'WTI\s*Crude\s*Oil', r'West\s*Texas'],
                   r'\$\s*/\s*bbl', 'usd_bbl'),
        ('opec',   [r'Opec[\s\-]*Basket', r'OPEC[\s\-]*Basket'],
                   r'\$\s*/\s*bbl', 'usd_bbl'),
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
                break
    return out


def _extract_quarterly(html: str) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {}
    APOS = r"['\u2018\u2019\u201A\u201B`]"
    FILLER = r'[^0-9\u00d8]{0,120}'

    for m in re.finditer(
            rf"(I{{1,3}}V?|IV)\.\s*Quartal\s*{APOS}?(\d{{2}})"
            rf"{FILLER}[Ø\u00d8]\s*([0-9]+[,.][0-9]+)\s*EUR",
            html, re.IGNORECASE | re.DOTALL):
        roman, yy, val = m.group(1).upper(), m.group(2), m.group(3)
        roman_to_q = {'I': 1, 'II': 2, 'III': 3, 'IV': 4}
        q = roman_to_q.get(roman)
        if q:
            out[f'q{q}_20{yy}_eur_100l'] = _parse_de_decimal(val)

    for m in re.finditer(
            rf"gesamt\s+(\d{{4}}){FILLER}[Ø\u00d8]\s*([0-9]+[,.][0-9]+)\s*EUR",
            html, re.IGNORECASE | re.DOTALL):
        year, val = m.group(1), m.group(2)
        out[f'y{year}_eur_100l'] = _parse_de_decimal(val)

    return out


def _fetch_destatis_supplement() -> Dict[str, List[dict]]:
    """
    Pull historical heating oil from Destatis. Graceful — never raises.
    """
    result: Dict[str, List[dict]] = {
        'consumer_eur_100l': [],
        'producer_index':    [],
    }

    user  = os.environ.get('DESTATIS_USERNAME', '').strip()
    pwd   = os.environ.get('DESTATIS_PASSWORD', '')
    token = os.environ.get('DESTATIS_API_TOKEN', '').strip()
    if not (user and pwd) and not token:
        print('    destatis supplement: no credentials configured, skipping')
        return result

    # FIX v5.3: relative import within the fetchers package. The previous
    # `from destatis_vpi import ...` was absolute and resolved nowhere.
    try:
        from . import destatis_vpi as dv
    except ImportError as e:
        print(f'    destatis supplement: import failed ({e}), skipping')
        return result

    try:
        creds     = dv._Credentials.from_env()
        auth_mode = dv._check_login(creds)
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
            m_int = DE_MONTHS.get(mlbl)
            period = f'{year}-{m_int:02d}' if m_int else year
            try:
                pts.append({'period': period, 'v': float(str(value).replace(',', '.'))})
            except (ValueError, TypeError):
                continue
        pts.sort(key=lambda x: x['period'])
        return pts

    try:
        rows = dv._fetch_tablefile(
            creds, DESTATIS_TABLE_HEIZOIL_CONSUMER,
            startyear=2019, mode=auth_mode,
        )
        result['consumer_eur_100l'] = _rows_to_series(rows)
        print(f'    destatis {DESTATIS_TABLE_HEIZOIL_CONSUMER}: '
              f'{len(result["consumer_eur_100l"])} pts (Heizöl EUR/100L)')
    except Exception as e:
        print(f'    destatis {DESTATIS_TABLE_HEIZOIL_CONSUMER}: {e}')

    try:
        rows = dv._fetch_tablefile(
            creds, DESTATIS_TABLE_HEIZOIL_INDEX,
            startyear=2019, mode=auth_mode,
        )
        result['producer_index'] = _rows_to_series(rows)
        print(f'    destatis {DESTATIS_TABLE_HEIZOIL_INDEX}: '
              f'{len(result["producer_index"])} pts (Erzeugerpreisindex)')
    except Exception as e:
        print(f'    destatis {DESTATIS_TABLE_HEIZOIL_INDEX}: {e}')

    return result


def fetch() -> dict:
    s = http.get_session()
    r = s.get(URL, timeout=25, headers={'Accept': 'text/html,application/xhtml+xml'})
    r.raise_for_status()

    html = _normalize(r.text)

    ref_price = _extract_reference_price(html)
    if ref_price is None:
        raise RuntimeError('Tecson: could not extract reference price')

    if not validators.in_range('heating_oil_eur_l', ref_price):
        raise RuntimeError(
            f'Tecson: reference price {ref_price:.4f} EUR/L outside allowed range'
        )

    changes   = _extract_change_table(html)
    notations = _extract_oil_notations(html)
    quarterly = _extract_quarterly(html)

    # FIX v5.3: removed broken q_count one-liner (iter(list, sentinel) crash).
    q_total  = sum(1 for k in quarterly if k.startswith('q'))
    yr_count = sum(1 for k in quarterly if k.startswith('y'))

    print(
        f'    tecson: ref={ref_price} EUR/L'
        f', brent={notations.get("brent_usd_bbl")}'
        f', wti={notations.get("wti_usd_bbl")}'
        f', gasoil={notations.get("gasoil_eur_t")}'
        f', quarterly={q_total} entries'
        f', annual={yr_count} years'
    )

    history.record_history('heating_oil', {
        'ref_eur_l':     ref_price,
        'brent_usd_bbl': notations.get('brent_usd_bbl'),
        'wti_usd_bbl':   notations.get('wti_usd_bbl'),
        'gasoil_eur_t':  notations.get('gasoil_eur_t'),
    })

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
                f'{DESTATIS_TABLE_HEIZOIL_INDEX} (producer index 2015=100)'
            ),
            'license':      'Tecson: attribution required. Destatis: DL-DE-BY-2.0.',
            'units':        'EUR per liter for heating oil; per 100L for quarterly averages',
            'price_basis':  '3000 L delivery, sulphur-poor, incl. 19% VAT',
        },
    }
