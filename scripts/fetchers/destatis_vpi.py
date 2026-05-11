"""
Destatis Genesis-Online — VPI Energie (consumer price index for energy).

Implemented per the official "Beispiele für POST-Anfragen an die
RESTful/JSON-Schnittstelle mit Python", May 2025.

Key facts from the docs:
  - GET requests were turned off on 30 June 2025. POST-only now.
  - Auth: HTTP headers `username` (= API token) and `password` (empty).
    Content-Type must be 'application/x-www-form-urlencoded'.
  - Test endpoint: POST helloworld/logincheck → returns
    {"Status":"Sie wurden erfolgreich an- und abgemeldet!..."}
  - Table fetch: POST data/tablefile (NOT data/table — that endpoint
    doesn't exist).
  - Recommended format for table fetch: 'ffcsv' (Flatfile CSV).
  - With compress=true the response is a ZIP archive containing one CSV.
  - Tables exceeding 40k rows must be split or batch-queued.

Table codes used:
  61111-0004  Verbraucherpreisindex — Sondergliederung COICOP-5-Steller
              (monthly, includes energy categories: Strom, Gas, Heizöl, Kraftstoffe...)
  61111-0001  Verbraucherpreisindex Deutschland insgesamt (headline)

Update frequency: monthly, around the 10th-15th, so we run once per day
and the data refreshes when Destatis publishes the new month.

Token: register at https://www-genesis.destatis.de → Mein Konto → API.
"""
import io
import os
import zipfile
from typing import Dict, List, Optional

import csv as csv_mod

from core import http, history


BASE_URL = 'https://www-genesis.destatis.de/genesisWS/rest/2020'

TABLE_VPI_DETAIL   = '61111-0004'   # COICOP-5-Steller, monatlich
TABLE_VPI_HEADLINE = '61111-0001'   # Deutschland insgesamt

# Energy-relevant labels within the detail table. We match by substring
# in the variable_attribute_label column. Case-insensitive.
ENERGY_KEYWORDS = [
    'strom', 'gas', 'heizöl', 'heizoel',
    'kraftstoffe', 'fernwärme', 'fernwaerme',
    'energie', 'flüssige brennstoffe', 'feste brennstoffe',
]


def _to_float(s) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip().replace(',', '.')
    if not s or s in ('.', '-', '...', 'x', '/'):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _post(endpoint: str, token: str, **params) -> 'requests.Response':
    """
    POST to a Genesis endpoint with header-auth.

    Per the May-2025 spec, credentials go in HTTP headers `username` and
    `password`. With an API token, username = token and password = "".

    Some HTTP libraries strip empty headers, which the Destatis server
    misinterprets as "no password sent". To work around this we send a
    single space rather than empty string for the password header.
    """
    s = http.get_session()
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'username': token.strip(),  # strip any whitespace from secret
        'password': ' ',             # single space, not '' — survives header normalization
    }
    body = {'language': 'de', **params}
    timeout = 30 if 'helloworld' in endpoint else 90
    r = s.post(f'{BASE_URL}/{endpoint}',
               data=body, headers=headers, timeout=timeout)
    return r


def _post_body_auth(endpoint: str, token: str, **params) -> 'requests.Response':
    """
    Alternative auth: token in the request body, not headers.
    Some older Destatis examples (and the bundesAPI client) put auth into
    the body. We use this as a fallback if header-auth fails.
    """
    s = http.get_session()
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    body = {
        'username': token.strip(),
        'password': '',
        'language': 'de',
        **params,
    }
    timeout = 30 if 'helloworld' in endpoint else 90
    r = s.post(f'{BASE_URL}/{endpoint}',
               data=body, headers=headers, timeout=timeout)
    return r


def _check_login(token: str) -> str:
    """
    Auth probe via POST helloworld/logincheck.

    Two attempts:
      1. Header-auth (as per the official May-2025 spec)
      2. Body-auth (used by older clients and some unofficial docs)
    Returns the auth mode that worked ('header' or 'body'),
    raises descriptive RuntimeError if both fail.
    """
    token = (token or '').strip()
    if len(token) < 10:
        raise RuntimeError(f'Destatis token too short ({len(token)} chars). '
                           'Get token from Mein Konto → Webservice/API.')
    if ' ' in token or '\n' in token:
        raise RuntimeError('Destatis token contains whitespace/newline. '
                           'Re-paste the secret without trailing whitespace.')

    last_error = None

    # Attempt 1: header-auth (spec-compliant)
    try:
        r = _post('helloworld/logincheck', token)
        if r.ok:
            payload = r.json()
            status = payload.get('Status', '')
            if 'erfolgreich' in status.lower():
                print(f'    destatis auth: header-mode ok')
                return 'header'
            last_error = f'header-auth status: {status[:200]}'
        else:
            last_error = f'header-auth HTTP {r.status_code}'
    except Exception as e:
        last_error = f'header-auth exception: {e}'
    print(f'    destatis header-auth failed ({last_error[:120]}), trying body-auth...')

    # Attempt 2: body-auth fallback
    try:
        r = _post_body_auth('helloworld/logincheck', token)
        if r.ok:
            payload = r.json()
            status = payload.get('Status', '')
            if 'erfolgreich' in status.lower():
                print(f'    destatis auth: body-mode ok')
                return 'body'
            raise RuntimeError(
                f'Destatis auth: BOTH modes failed. '
                f'header: {last_error[:120]}; body: {status[:200]}'
            )
        raise RuntimeError(f'body-auth HTTP {r.status_code}: {r.text[:120]}')
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(
            f'Destatis auth: BOTH modes failed. '
            f'header: {last_error[:120]}; body: {e}'
        )


def _post_auto(endpoint: str, token: str, mode: str, **params) -> 'requests.Response':
    """Dispatch to header-auth or body-auth based on detected mode."""
    if mode == 'body':
        return _post_body_auth(endpoint, token, **params)
    return _post(endpoint, token, **params)


def _fetch_tablefile_split(token: str, table_name: str,
                           startyear: int,
                           classifyingvariable1: Optional[str],
                           classifyingkey1: Optional[str],
                           mode: str = 'header') -> List[dict]:
    """
    Fall-back path for tables that exceed Destatis' 40k-row direct-fetch
    limit. Per the official spec (May 2025), the recommended workaround is
    to split the time axis. We fetch year-by-year in 2-year chunks and
    concatenate.
    """
    from datetime import datetime
    current_year = datetime.now().year
    all_rows: List[dict] = []
    chunk_start = startyear
    while chunk_start <= current_year:
        chunk_end = min(chunk_start + 1, current_year)
        try:
            chunk = _fetch_tablefile(
                token, table_name,
                startyear=chunk_start, endyear=chunk_end,
                classifyingvariable1=classifyingvariable1,
                classifyingkey1=classifyingkey1,
                mode=mode,
            )
            print(f'    destatis {table_name} {chunk_start}-{chunk_end}: '
                  f'{len(chunk)} rows')
            all_rows.extend(chunk)
        except Exception as e:
            print(f'  ! destatis {table_name} {chunk_start}-{chunk_end}: {e}')
        chunk_start = chunk_end + 1
    return all_rows


def _fetch_tablefile(token: str, table_name: str,
                     startyear: int = 2020,
                     endyear: Optional[int] = None,
                     classifyingvariable1: Optional[str] = None,
                     classifyingkey1: Optional[str] = None,
                     mode: str = 'header') -> List[dict]:
    """
    Download a table via POST data/tablefile in ffcsv format, ZIP-compressed.
    Decompress and parse to a list of dicts.

    `mode` selects between header-auth ('header') and body-auth ('body').
    """
    params: Dict[str, object] = {
        'name': table_name,
        'startyear': startyear,
        'compress': 'true',
        'format': 'ffcsv',
    }
    if endyear is not None:
        params['endyear'] = endyear
    if classifyingvariable1:
        params['classifyingvariable1'] = classifyingvariable1
    if classifyingkey1:
        params['classifyingkey1'] = classifyingkey1

    r = _post_auto('data/tablefile', token, mode, **params)
    if not r.ok:
        body = r.text[:300] if r.text else ''
        raise RuntimeError(f'Destatis tablefile {table_name} HTTP {r.status_code}: {body}')

    # Three possible response shapes:
    #   1. ZIP binary (compress=true successful)
    #   2. Plain CSV (compress=true ignored or table small)
    #   3. JSON envelope with an error code (e.g. "table too large", code 98)
    content = r.content
    content_type = r.headers.get('Content-Type', '').lower()

    # Detect JSON-error envelope first
    if content[:1] == b'{':
        try:
            envelope = r.json()
            status = envelope.get('Status') or {}
            if isinstance(status, dict) and status.get('Code', 0) >= 90:
                # Code 98 = "Tabelle zu gross" — the official advice is to
                # split by year. Try that automatically if endyear wasn't set.
                code = status.get('Code')
                msg = status.get('Content', '')[:200]
                if code == 98 and endyear is None:
                    print(f'    destatis {table_name}: too big, splitting by year')
                    return _fetch_tablefile_split(
                        token, table_name, startyear,
                        classifyingvariable1, classifyingkey1, mode=mode,
                    )
                raise RuntimeError(
                    f'Destatis tablefile {table_name} status code {code}: {msg}'
                )
        except (ValueError, RuntimeError) as e:
            if isinstance(e, RuntimeError):
                raise

    # Try ZIP first (the common case with compress=true)
    csv_text: Optional[str] = None
    if content[:2] == b'PK':
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            names = zf.namelist()
            if not names:
                raise RuntimeError(f'Destatis tablefile {table_name}: empty ZIP')
            with zf.open(names[0]) as f:
                csv_text = f.read().decode('utf-8', errors='replace')
    elif 'text/csv' in content_type or content[:6] == b'\xef\xbb\xbf' \
            or b';' in content[:200]:
        csv_text = content.decode('utf-8', errors='replace')
    else:
        # Last resort: assume utf-8 text
        csv_text = content.decode('utf-8', errors='replace')

    if not csv_text:
        raise RuntimeError(f'Destatis tablefile {table_name}: could not extract CSV')

    return _parse_ffcsv(csv_text)


def _parse_ffcsv(csv_text: str) -> List[dict]:
    """
    Parse a Destatis ffcsv (Flatfile CSV) into a list of dict rows.
    Decimal separator is comma, list separator is semicolon, na markers
    are '...', '.', '-', '/', 'x'.
    """
    reader = csv_mod.reader(io.StringIO(csv_text), delimiter=';')
    rows: List[dict] = []
    header: Optional[List[str]] = None
    for row in reader:
        if not row:
            continue
        if header is None:
            header = row
            continue
        # Skip footer / decoration lines
        if row[0].startswith('_') or row[0].startswith('Quelle'):
            continue
        d = {}
        for i, key in enumerate(header):
            d[key] = row[i] if i < len(row) else None
        rows.append(d)
    return rows


def _filter_energy(rows: List[dict]) -> List[dict]:
    """Keep only rows whose variable_attribute_label mentions energy categories."""
    out = []
    for r in rows:
        # The label can live in different columns depending on the table layout.
        # Check the most likely fields.
        lbl = (
            r.get('1_variable_attribute_label')
            or r.get('2_variable_attribute_label')
            or r.get('3_variable_attribute_label')
            or ''
        ).lower()
        if any(kw in lbl for kw in ENERGY_KEYWORDS):
            out.append(r)
    return out


def _build_series(rows: List[dict]) -> Dict[str, List[dict]]:
    """
    Group rows into time series by category label.
    Returns: { '<category>': [{period: 'YYYY-MM', v: float}, ...], ... }

    The ffcsv format uses 'time' and 'time_label' columns. For monthly data
    'time' is the year and 'time_label' is the month name (e.g. 'Januar').
    """
    DE_MONTHS = {
        'januar': 1, 'februar': 2, 'märz': 3, 'maerz': 3, 'april': 4,
        'mai': 5, 'juni': 6, 'juli': 7, 'august': 8, 'september': 9,
        'oktober': 10, 'november': 11, 'dezember': 12,
    }
    series: Dict[str, List[dict]] = {}
    for r in rows:
        label = (
            r.get('1_variable_attribute_label')
            or r.get('2_variable_attribute_label')
            or r.get('3_variable_attribute_label')
        )
        if not label:
            continue
        # Period: e.g. time=2024 + time_label=Januar -> "2024-01"
        year = (r.get('time') or '').strip()
        month_lbl = (r.get('time_label') or '').strip().lower()
        if not year:
            continue
        if month_lbl in DE_MONTHS:
            period = f'{year}-{DE_MONTHS[month_lbl]:02d}'
        else:
            period = year
        v = _to_float(r.get('value'))
        if v is None:
            continue
        series.setdefault(label, []).append({'period': period, 'v': v})

    # Sort each series chronologically
    for k in list(series.keys()):
        series[k].sort(key=lambda x: x['period'])
    return series


def fetch() -> dict:
    token = os.environ.get('DESTATIS_API_TOKEN', '').strip()
    if len(token) < 10:
        raise RuntimeError('DESTATIS_API_TOKEN missing — register at www-genesis.destatis.de')

    # Auth probe — returns the working auth mode ('header' or 'body')
    auth_mode = _check_login(token)

    # Headline VPI (Deutschland insgesamt)
    headline_series: Dict[str, List[dict]] = {}
    try:
        head_rows = _fetch_tablefile(token, TABLE_VPI_HEADLINE, startyear=2020, mode=auth_mode)
        headline_series = _build_series(head_rows)
        print(f'    destatis headline {TABLE_VPI_HEADLINE}: {len(head_rows)} rows, '
              f'{len(headline_series)} series')
    except Exception as e:
        print(f'  ! destatis headline {TABLE_VPI_HEADLINE}: {e}')

    # Detail with energy breakdown.
    detail_rows: List[dict] = []
    try:
        detail_rows = _fetch_tablefile(
            token, TABLE_VPI_DETAIL,
            startyear=2020,
            classifyingvariable1='CC13A5',
            mode=auth_mode,
        )
    except Exception as e:
        print(f'  ! destatis detail {TABLE_VPI_DETAIL}: {e}')

    energy_rows = _filter_energy(detail_rows)
    print(f'    destatis detail: {len(detail_rows)} total rows, '
          f'{len(energy_rows)} energy-tagged')
    energy_series = _build_series(energy_rows)

    # Latest snapshot
    latest = {}
    for cat, pts in energy_series.items():
        if pts:
            latest[cat] = pts[-1]

    # History append: capture the latest reading per category
    if latest:
        try:
            hist_record = {}
            for cat, p in latest.items():
                # Sanitize category name to a valid jsonl key
                key = cat.lower()
                key = ''.join(c if c.isalnum() else '_' for c in key)[:60]
                hist_record[key] = p['v']
            history.record_history('destatis_vpi', hist_record)
        except Exception as e:
            print(f'  ! destatis history append: {e}')

    return {
        'data': {
            'headline_vpi': headline_series,
            'energy_series': energy_series,
            'latest': latest,
        },
        'meta': {
            'source': 'Statistisches Bundesamt (Destatis), GENESIS-Online',
            'tables': [TABLE_VPI_HEADLINE, TABLE_VPI_DETAIL],
            'license': 'Datenlizenz Deutschland Namensnennung 2.0',
            'units': 'Index 2020=100',
            'note': 'Implemented per official Destatis Genesis API spec, May 2025',
        },
    }
