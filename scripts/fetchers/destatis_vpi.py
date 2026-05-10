"""
Destatis Genesis-Online — VPI Energie (consumer price index for energy).

Table 61111-0006 (VPI Sondergliederungen) contains monthly index values
for energy categories: Strom, Gas, Heizöl, Kraftstoffe, Fernwärme.

Genesis API switched to POST-only in July 2025. Auth via username=<token>.
The server returns CSV inside a JSON wrapper (`Object.Content`).

Token: register at https://www-genesis.destatis.de -> "Mein Konto" -> API.
We pass the token as the `username` field — that's how Destatis does it.

Update frequency: monthly, around the 10th-15th.
"""
import csv
import io as stdio
import os
import re
from typing import Dict, List, Optional

from core import http


BASE_URL = 'https://www-genesis.destatis.de/genesisWS/rest/2020'
TABLE_VPI_DETAIL = '61111-0006'  # Sondergliederungen incl. energy categories
TABLE_VPI_HEADLINE = '61111-0001'  # Headline VPI Deutschland insgesamt

# Energy-relevant DESC values within table 61111-0006. We filter to these.
ENERGY_KEYWORDS = [
    'energie', 'strom', 'gas', 'heizöl', 'heizoel',
    'kraftstoffe', 'fernwärme', 'fernwaerme', 'umweltökonom',
]


def _to_float(s: str) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip().replace(',', '.')
    if not s or s in ('.', '-', '...'):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _request(method: str, endpoint: str, token: str, **params) -> dict:
    """
    Genesis-Online v5.0 API. Auth via headers, parameters via query (GET) or body (POST).

    Long timeout because Destatis API is notoriously slow at peak times
    (queries to certain large tables routinely take 30-60s server-side).
    Retries once on transport-level errors (timeout, connection reset).
    """
    s = http.get_session()
    headers = {
        'username': token,
        'password': '',
    }
    url = f'{BASE_URL}/{endpoint}'
    # Genesis is slow. Use 90s for data calls, 30s for auth probes.
    timeout = 30 if 'helloworld' in endpoint else 90

    last_exc = None
    for attempt in (1, 2):
        try:
            if method.upper() == 'GET':
                r = s.get(url, headers=headers,
                          params={'language': 'de', **params}, timeout=timeout)
            else:
                headers['Content-Type'] = 'application/x-www-form-urlencoded'
                r = s.post(url, data={'language': 'de', **params},
                           headers=headers, timeout=timeout)
            if not r.ok:
                body = r.text[:300] if r.text else ''
                raise RuntimeError(f'Destatis HTTP {r.status_code} on {method} {endpoint}: {body}')
            return r.json()
        except Exception as e:
            last_exc = e
            err = str(e).lower()
            transient = ('timeout' in err or 'timed out' in err
                         or 'connection' in err or 'remote end closed' in err)
            if attempt == 1 and transient:
                print(f'    destatis {endpoint} attempt 1 failed ({type(e).__name__}); '
                      f'retrying with same timeout')
                continue
            raise
    raise last_exc  # type: ignore[misc]


def _post(endpoint: str, token: str, **params) -> dict:
    """Backwards-compatible wrapper used by data/* endpoints."""
    return _request('POST', endpoint, token, **params)


def _parse_table_csv(csv_text: str) -> List[dict]:
    """
    Genesis returns CSV with header, data rows, and trailing footer ('__________').
    Format example: Statistik_Code;Statistik_Label;Zeit_Code;Zeit_Label;Zeit;...
    """
    rows = []
    reader = csv.reader(stdio.StringIO(csv_text), delimiter=';')
    header = None
    for row in reader:
        if not row or row[0].startswith('_'):
            continue
        if header is None:
            header = row
            continue
        rows.append(dict(zip(header, row)))
    return rows


def _filter_energy(rows: List[dict]) -> List[dict]:
    """Keep only rows whose first 'Auspraegung_Label' mentions an energy term."""
    out = []
    for r in rows:
        label = ''
        for k, v in r.items():
            if 'Auspraegung_Label' in k or 'Merkmal_Label' in k:
                label = (label + ' ' + str(v)).lower()
        if any(kw in label for kw in ENERGY_KEYWORDS):
            out.append(r)
    return out


def _build_series(rows: List[dict]) -> Dict[str, List[dict]]:
    """
    Pivot rows into per-category time series.
    Genesis monthly data uses Zeit='YYYY-MM' or Zeit_Label='Januar 2025'.
    """
    series: Dict[str, List[dict]] = {}
    for r in rows:
        # Find the value column (label contains 'Verbraucherpreisindex')
        value = None
        for k, v in r.items():
            if 'Verbraucherpreisindex' in k or k.startswith('PREIS1') or k.endswith('2020=100'):
                fv = _to_float(v)
                if fv is not None:
                    value = fv
                    break
        if value is None:
            continue
        # Time
        period = r.get('Zeit') or r.get('Zeit_Label') or ''
        period = str(period).strip()
        if not re.match(r'^\d{4}', period):
            continue
        # Category label
        cat = ''
        for k, v in r.items():
            if 'Auspraegung_Label' in k:
                cat = str(v)
                break
        if not cat:
            continue
        series.setdefault(cat, []).append({'period': period, 'v': value})
    for cat in series:
        series[cat].sort(key=lambda x: x['period'])
    return series


def _fetch_table(token: str, table_name: str, start_year: str = '2020') -> List[dict]:
    """Fetch one table and return parsed rows."""
    js = _post('data/table', token,
               name=table_name,
               area='all',
               compress='false',
               startyear=start_year,
               format='csv')
    if not isinstance(js, dict) or 'Object' not in js:
        status = js.get('Status', {}) if isinstance(js, dict) else {}
        raise RuntimeError(f'Destatis {table_name}: unexpected response, status={status}')
    content = js['Object'].get('Content', '')
    if not content:
        raise RuntimeError(f'Destatis {table_name}: empty Content')
    return _parse_table_csv(content)


def fetch() -> dict:
    token = os.environ.get('DESTATIS_API_TOKEN', '').strip()
    if len(token) < 10:
        raise RuntimeError('DESTATIS_API_TOKEN missing — register at www-genesis.destatis.de')

    # Auth probe: 'helloworld/logincheck' is the official auth-test endpoint.
    # We GET it (POST returns 405). On success the response contains
    # {"Status":"Sie wurden erfolgreich an- und abgemeldet."}.
    # Fall back to whoami (GET) if logincheck doesn't exist on this endpoint version.
    try:
        probe = _request('GET', 'helloworld/logincheck', token)
        ident = probe.get('Status', probe.get('User', '?')) if isinstance(probe, dict) else '?'
        print(f'    destatis logincheck: {ident}')
    except Exception as e1:
        try:
            probe = _request('GET', 'helloworld/whoami', token)
            ident = probe.get('User', probe.get('Ident', '?')) if isinstance(probe, dict) else '?'
            print(f'    destatis whoami: {ident}')
        except Exception as e2:
            raise RuntimeError(
                f'Destatis auth probe failed: logincheck → {e1}; whoami → {e2}. '
                'Check DESTATIS_API_TOKEN at https://www-genesis.destatis.de '
                '(Mein Konto → Webservice/API).'
            )

    # Headline VPI: gives us the overall inflation context
    try:
        head_rows = _fetch_table(token, TABLE_VPI_HEADLINE, start_year='2020')
        headline_series = _build_series(head_rows)
    except Exception as e:
        print(f'  ! destatis headline {TABLE_VPI_HEADLINE}: {e}')
        headline_series = {}

    # Detail with energy breakdown
    detail_rows = _fetch_table(token, TABLE_VPI_DETAIL, start_year='2020')
    energy_rows = _filter_energy(detail_rows)
    print(f'    destatis: {len(detail_rows)} total rows, {len(energy_rows)} energy-tagged')
    energy_series = _build_series(energy_rows)

    # Latest snapshot for KPIs
    latest = {}
    for cat, pts in energy_series.items():
        if pts:
            latest[cat] = pts[-1]

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
        },
    }
