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

# Candidate tables for a consumer heating-oil price in EUR. The first is the
# one this fetcher has always used; it is rejected by Destatis with status
# 104 (no objects for the selection), so the others are tried after it and
# whichever answers is recorded in the output.
DESTATIS_TABLES_HEIZOIL_CONSUMER = ['43531-0005', '61241-0004', '61241-0002']
DESTATIS_TABLE_HEIZOIL_CONSUMER = DESTATIS_TABLES_HEIZOIL_CONSUMER[0]
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


def _price_candidates(html: str) -> List[dict]:
    """
    Every number in the page that is denominated in a heating-oil price unit,
    normalised to EUR per litre.

    The previous extractor ran five fixed regexes and gave up if none hit,
    which is what happened when tecson.de changed its markup: the fetcher had
    been failing since 2026-09-14 with nothing in the log but "could not
    extract reference price". Scanning for unit-bearing numbers and scoring
    them survives a layout change, because the unit is the part that cannot
    change without the page ceasing to be about heating-oil prices.
    """
    # Up to three decimals: heating oil is quoted as 1,045 EUR/l as readily
    # as 1,04. Allowing only two made the regex match the "045" of "1,045"
    # as a standalone number, which then failed the range check and dropped
    # the real reference price while leaving the regional ones standing.
    # The lookbehind stops a match from starting inside a longer number.
    NUM = r'(?<![0-9.,])([0-9]{1,4}(?:[.,][0-9]{1,3})?)'
    units = [
        (NUM + r'\s*(?:€|EUR)\s*/\s*(?:l|Liter)\b', 1.0),
        (NUM + r'\s*(?:Cent|ct)\s*/\s*(?:l|Liter)\b', 0.01),
        (NUM + r'\s*(?:€|EUR)\s*/\s*100\s*(?:l|Liter)\b', 0.01),
        (NUM + r'\s*(?:€|EUR)\s*(?:je|pro)\s*100\s*(?:l|Liter)\b', 0.01),
    ]
    out: List[dict] = []
    for pattern, factor in units:
        for m in re.finditer(pattern, html, re.IGNORECASE):
            val = _parse_de_decimal(m.group(1))
            if val is None:
                continue
            eur_l = round(val * factor, 4)
            if not (0.30 < eur_l < 5.00):
                continue
            ctx = re.sub(r'\s+', ' ', html[max(0, m.start() - 120): m.end() + 40])
            out.append({'eur_l': eur_l, 'context': ctx, 'pos': m.start()})
    return out


# Wording that marks a candidate as THE reference price rather than one of
# the many regional or volume-tier prices the page also lists.
_REF_HINTS = (
    (r'Ø|\bDurchschnitt', 3),
    (r'Referenzpreis|Vergleichspreis', 4),
    (r'\bHEL\b|Heiz[öo]l', 2),
    (r'3\.?000\s*(?:l|Liter)', 3),
    (r'bundesweit|Deutschland', 2),
)


def _extract_reference_price(html: str) -> Optional[float]:
    candidates = _price_candidates(html)
    if not candidates:
        return None
    for c in candidates:
        c['score'] = sum(w for pat, w in _REF_HINTS
                         if re.search(pat, c['context'], re.IGNORECASE))
    # Highest score wins; ties go to the earliest occurrence, since the page
    # leads with its headline figure.
    best = sorted(candidates, key=lambda c: (-c['score'], c['pos']))[0]
    return best['eur_l'] if best['score'] > 0 else None


def _log_price_diagnostics(html: str) -> None:
    """
    Print what the page offers when no price could be read, so the next
    scheduled run shows the current markup instead of just failing again.
    """
    candidates = _price_candidates(html)
    print(f'    tecson: {len(html)} bytes, {len(candidates)} unit-bearing '
          f'numbers in the plausible range')
    for c in candidates[:8]:
        print(f'        {c["eur_l"]} EUR/l  <- ...{c["context"][-110:]}')
    if not candidates:
        # No units at all: show where prices would normally be, to tell a
        # layout change apart from a block page or a cookie wall.
        for kw in ('Heizöl', 'Cent', 'Preis', 'captcha', 'Cookie'):
            hits = [m.start() for m in re.finditer(kw, html, re.IGNORECASE)][:2]
            for h in hits:
                snippet = re.sub(r'\s+', ' ', html[h: h + 130])
                print(f'        "{kw}" @{h}: {snippet}')


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

    # Use destatis_vpi's series builder rather than a private copy.
    # The copy that stood here carried the same bug the VPI parser had: it
    # read the month from `time_label`, but GENESIS ffcsv puts the month in
    # its own numbered variable slot (MONAT), and it had no notion of the
    # value variable. Both tables report an index AND its year-on-year change
    # rate, one row each, so the two were interleaved under the same period —
    # producer_index read 2019=92.1, 2019=1.1, 2020=91.2, 2020=-1.0.
    def _series_from_table(rows: List[dict], prefer: str) -> List[dict]:
        """
        Build one clean series from a GENESIS table.

        `prefer` is a substring of the wanted measure ('index' or 'preis');
        change-rate series are never chosen, because they are a different
        quantity that happens to share the period axis.
        """
        built = dv._build_series(rows)
        if not built:
            return []
        keys = list(built.keys())
        def is_rate(k: str) -> bool:
            return bool(re.search(r'veränderung|vorjahr|%', k, re.IGNORECASE))
        candidates = [k for k in keys if not is_rate(k)] or keys
        wanted = [k for k in candidates if prefer.lower() in k.lower()]
        pool = wanted or candidates

        # Guard on the values as well as the key. A key can be unhelpful, but
        # a year-on-year rate and a rebased index never look alike: the rate
        # goes negative and sits near zero, the index does not. Picking the
        # rate and calling it an index is what produced 1.1, -1.0, 9.6, 29.8
        # where 92.1, 91.2, 100.0, 129.8 belonged.
        def looks_like_rate(key: str) -> bool:
            vals = [p['v'] for p in built[key]]
            if not vals:
                return False
            return any(v < 0 for v in vals) and max(abs(v) for v in vals) < 60

        if prefer.lower() in ('index', 'preis'):
            not_rates = [k for k in pool if not looks_like_rate(k)]
            if not_rates:
                pool = not_rates
        chosen = max(pool, key=lambda k: len(built[k]))
        if len(keys) > 1:
            print(f'      table series: {keys} -> using {chosen!r}')
        return built[chosen]

    # 43531-0005 answers "status code 104: Es gibt keine Objekte zum
    # angegebenen Selektionskriterium" — the table does not serve this
    # selection, which is why the consumer series had always been empty. It
    # stays first in case that changes; the alternatives are tried after it.
    consumer_error: Optional[str] = None
    for table in DESTATIS_TABLES_HEIZOIL_CONSUMER:
        try:
            rows = dv._fetch_tablefile(creds, table, startyear=2019, mode=auth_mode)
            series = _series_from_table(rows, 'preis')
            if series:
                result['consumer_eur_100l'] = series
                result['consumer_table'] = table
                print(f'    destatis {table}: {len(series)} pts (Heizöl EUR/100L)')
                break
            print(f'    destatis {table}: {len(rows)} rows but no price series'
                  + (f'; columns: {list(rows[0].keys())[:18]}' if rows else ''))
        except Exception as e:
            consumer_error = f'{table}: {str(e)[:120]}'
            print(f'    destatis {table}: {str(e)[:150]}')
    if not result['consumer_eur_100l']:
        result['consumer_unavailable'] = consumer_error or 'keine der Tabellen lieferte Preise'

    try:
        rows = dv._fetch_tablefile(
            creds, DESTATIS_TABLE_HEIZOIL_INDEX,
            startyear=2019, mode=auth_mode,
        )
        result['producer_index'] = _series_from_table(rows, 'index')
        print(f'    destatis {DESTATIS_TABLE_HEIZOIL_INDEX}: '
              f'{len(result["producer_index"])} pts (Erzeugerpreisindex)')
    except Exception as e:
        print(f'    destatis {DESTATIS_TABLE_HEIZOIL_INDEX}: {e}')

    return result


def fetch() -> dict:
    # Tecson is scraped, and a scrape is the least durable thing here: it had
    # been failing since 2026-09-14 and took the whole source down with it,
    # although the Destatis tables below need nothing from that page. The
    # scrape is now best-effort, and Destatis carries the price when it fails.
    html = ''
    tecson_error: Optional[str] = None
    ref_price: Optional[float] = None
    changes: Dict[str, Optional[float]] = {}
    notations: Dict[str, Optional[float]] = {}
    quarterly: Dict[str, Optional[float]] = {}

    try:
        s = http.get_session()
        r = s.get(URL, timeout=25, headers={'Accept': 'text/html,application/xhtml+xml'})
        r.raise_for_status()
        html = _normalize(r.text)
        ref_price = _extract_reference_price(html)
        if ref_price is None:
            tecson_error = 'kein Referenzpreis im Seitenquelltext gefunden'
            _log_price_diagnostics(html)
        elif not validators.in_range('heating_oil_eur_l', ref_price):
            tecson_error = f'Referenzpreis {ref_price:.4f} EUR/L außerhalb des Plausibilitätsbereichs'
            ref_price = None
        changes   = _extract_change_table(html)
        notations = _extract_oil_notations(html)
        quarterly = _extract_quarterly(html)
    except Exception as e:
        tecson_error = f'{type(e).__name__}: {e}'
        print(f'  ! tecson: {tecson_error}')

    # FIX v5.3: removed broken q_count one-liner (iter(list, sentinel) crash).
    q_total  = sum(1 for k in quarterly if k.startswith('q'))
    yr_count = sum(1 for k in quarterly if k.startswith('y'))

    print(
        f'    tecson: ref={ref_price if ref_price is not None else "—"} EUR/L'
        f', brent={notations.get("brent_usd_bbl")}'
        f', wti={notations.get("wti_usd_bbl")}'
        f', gasoil={notations.get("gasoil_eur_t")}'
        f', quarterly={q_total} entries'
        f', annual={yr_count} years'
    )

    destatis = _fetch_destatis_supplement()

    # Fall back to the official consumer price when the scrape gives nothing.
    # Destatis 43531-0005 reports EUR per 100 litres, monthly, with about two
    # months' lag — older than Tecson's daily figure, but real, and better
    # than a card frozen on a value from before the scraper broke.
    price_source = 'tecson'
    price_period: Optional[str] = None
    if ref_price is None:
        consumer = destatis.get('consumer_eur_100l') or []
        if consumer:
            latest = consumer[-1]
            candidate = round(latest['v'] / 100.0, 4)
            if validators.in_range('heating_oil_eur_l', candidate):
                ref_price = candidate
                price_source = 'destatis'
                price_period = latest.get('period')
                print(f'    heating_oil: Tecson lieferte nichts, nutze Destatis '
                      f'{price_period} = {ref_price} EUR/L')

    if ref_price is None:
        raise RuntimeError(
            'Heizölpreis aus keiner Quelle verfügbar — '
            f'Tecson: {tecson_error or "unbekannt"}; '
            f'Destatis-Verbraucherpreise: {len(destatis.get("consumer_eur_100l") or [])} Werte'
        )

    history.record_history('heating_oil', {
        'ref_eur_l':     ref_price,
        'price_source':  price_source,
        'brent_usd_bbl': notations.get('brent_usd_bbl'),
        'wti_usd_bbl':   notations.get('wti_usd_bbl'),
        'gasoil_eur_t':  notations.get('gasoil_eur_t'),
    })

    return {
        'data': {
            'reference_price_eur_l':  ref_price,
            # Which source the headline price came from, so the dashboard can
            # say "Destatis, August" instead of implying a daily Tecson quote.
            'price_source':           price_source,
            'price_period':           price_period,
            'tecson_error':           tecson_error,
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
