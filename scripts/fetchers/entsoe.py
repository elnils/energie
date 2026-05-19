"""
ENTSO-E Transparency Platform — electricity prices, generation, cross-border flows.

Uses direct XML REST calls to the ENTSOE Transparency Platform API.
No external library required — pure stdlib xml.etree.ElementTree.

Data contract (matches index.html renderCrossBorder()):
  D.entsoe.awaiting_key  → bool, True if no token configured
  D.entsoe.flows_in      → {neighbour: [{ts, v}]}  (imports INTO DE)
  D.entsoe.flows_out     → {neighbour: [{ts, v}]}  (exports FROM DE)
  D.entsoe.prices        → {bzn: [{ts, v}]}
  D.entsoe.generation    → {fuel_type: [{ts, v}]}
  D.entsoe.load          → [{ts, v}]
  D.entsoe.net_position  → [{ts, v}]

Auth: ENTSOE_SECURITY_TOKEN environment variable.
Register at: https://transparency.entsoe.eu → Account → Web API Security Token

v5.3 fixes:
  - truthiness bug on ElementTree elements: `el1 or el2` is False when el1 has
    no children (which <price.amount> never has), causing every price point
    to be silently dropped. Replaced with explicit `is None` checks.
  - hardcoded namespace list: replaced with a regex that strips ALL default
    namespace declarations, so future schema versions (7.1, 7.3, ...) work
    without code changes.
"""
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
import xml.etree.ElementTree as ET

from core import http

TOKEN = os.environ.get('ENTSOE_SECURITY_TOKEN', '').strip()
BASE  = 'https://web-api.tp.entsoe.eu/api'

# Bidding zone EIC codes
BZN = {
    'DE_LU': '10Y1001A1001A82H',
    'AT':    '10YAT-APG------L',
    'FR':    '10YFR-RTE------C',
    'PL':    '10YPL-AREA-----S',
    'CH':    '10YCH-SWISSGRIDZ',
    'NL':    '10YNL----------L',
    'CZ':    '10YDOM-CZ-DE--D',
    'DK_1':  '10YDK-1--------W',
}

# Cross-border flow pairs (from DE_LU to each neighbour)
FLOW_PAIRS = [
    ('DE_LU', 'FR'), ('DE_LU', 'AT'), ('DE_LU', 'PL'),
    ('DE_LU', 'NL'), ('DE_LU', 'CZ'), ('DE_LU', 'DK_1'),
]

# PSRTYPE codes → readable labels for generation mix
PSRTYPE = {
    'B01': 'Biomass',      'B02': 'Fossil Brown coal/Lignite',
    'B03': 'Fossil Coal',  'B04': 'Fossil Gas',
    'B05': 'Fossil Hard coal', 'B06': 'Fossil Oil',
    'B09': 'Geothermal',   'B10': 'Hydro Pumped Storage',
    'B11': 'Hydro Run-of-River', 'B12': 'Hydro Water Reservoir',
    'B14': 'Nuclear',      'B15': 'Other Renewables',
    'B16': 'Solar',        'B17': 'Waste',
    'B18': 'Wind Offshore','B19': 'Wind Onshore', 'B20': 'Other',
}


# Strip ALL default namespace declarations in one pass. The previous
# version listed three hardcoded URIs and silently failed when ENTSO-E
# emitted a fourth (e.g. publicationdocument:7:3 instead of :7:0). This
# regex catches every `xmlns="..."` regardless of URI value.
_NS_DECL_RE = re.compile(r'\sxmlns="[^"]+"')


def _strip_default_ns(xml_text: str) -> str:
    return _NS_DECL_RE.sub('', xml_text)


def _fmt(dt: datetime) -> str:
    return dt.strftime('%Y%m%d%H%M')


def _to_ts(s: str) -> int:
    """Parse ENTSOE datetime string → unix seconds UTC."""
    s = s.rstrip('Z').replace('+00:00', '')
    for fmt in ('%Y-%m-%dT%H:%M', '%Y-%m-%dT%H:%M:%S', '%Y%m%d%H%M'):
        try:
            return int(datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            continue
    return 0


def _res_minutes(res_str: str) -> int:
    """PT60M → 60, PT15M → 15, P1D → 1440."""
    m = re.search(r'PT?(\d+)([HMD])', res_str or '')
    if not m:
        return 60
    n, u = int(m.group(1)), m.group(2)
    return n * 60 if u == 'H' else n if u == 'M' else n * 1440


def _parse_xml(xml_text: str, label_tag: Optional[str] = None) -> Dict[str, List[dict]]:
    """
    Parse ENTSOE publication XML into {label: [{ts, v}, ...]} dict.
    Works for prices (price.amount) and flows/generation/load (quantity).
    label_tag: child element name whose text becomes the series key; None = counter.
    """
    clean = _strip_default_ns(xml_text)
    try:
        root = ET.fromstring(clean)
    except ET.ParseError:
        return {}

    result: Dict[str, List[dict]] = {}
    counter = 0
    for ts_el in root.iter('TimeSeries'):
        # Determine series label (e.g. psrType for generation mix)
        label = str(counter)
        counter += 1
        if label_tag:
            el = ts_el.find(f'.//{label_tag}')
            if el is not None and el.text:
                label = el.text.strip()

        for period_el in ts_el.iter('Period'):
            start_el = period_el.find('.//start')
            res_el   = period_el.find('resolution')
            if start_el is None or not start_el.text:
                continue
            start_ts = _to_ts(start_el.text)
            res_min  = _res_minutes(res_el.text if res_el is not None else '')

            for pt in period_el.iter('Point'):
                pos_el = pt.find('position')
                # CRITICAL: explicit `is None` checks. `el1 or el2` is broken
                # for ElementTree elements without children (bool(el) == False
                # when len(el) == 0), so `pt.find('price.amount') or pt.find('quantity')`
                # silently discarded every single price point.
                val_el = pt.find('price.amount')
                if val_el is None:
                    val_el = pt.find('quantity')
                if pos_el is None or val_el is None or val_el.text is None:
                    continue
                try:
                    pos = int(pos_el.text) - 1  # 1-indexed → 0-indexed
                    val = float(val_el.text)
                    slot_ts = start_ts + pos * res_min * 60
                    result.setdefault(label, []).append({'ts': slot_ts, 'v': round(val, 2)})
                except (TypeError, ValueError):
                    continue

    # Deduplicate (overlapping periods can repeat timestamps) and sort
    for k in result:
        seen = {}
        for p in result[k]:
            seen[p['ts']] = p['v']
        result[k] = [{'ts': t, 'v': v} for t, v in sorted(seen.items())]
    return result


def _api(params: dict, timeout: int = 60) -> Optional[str]:
    s = http.get_session()
    p = {'securityToken': TOKEN, **params}
    try:
        r = s.get(BASE, params=p, timeout=timeout)
        if r.status_code == 429:
            time.sleep(30)
            r = s.get(BASE, params=p, timeout=timeout)
        if r.status_code == 204:
            return None   # valid request, no data for this window
        r.raise_for_status()
        return r.text
    except Exception as e:
        raise RuntimeError(str(e)) from e


def fetch() -> dict:
    if not TOKEN:
        print('    entsoe: no ENTSOE_SECURITY_TOKEN — awaiting_key mode')
        return {
            'data': {'awaiting_key': True},
            'meta': {
                'source': 'ENTSO-E Transparency Platform',
                'note': 'Set ENTSOE_SECURITY_TOKEN in GitHub Secrets. '
                        'Register at https://transparency.entsoe.eu',
            },
        }

    now       = datetime.now(timezone.utc)
    start_7d  = now - timedelta(days=7)
    start_14d = now - timedelta(days=14)

    # ── Prices (A44, day-ahead) ───────────────────────────────────────
    prices: Dict[str, List[dict]] = {}
    for name, bzn_code in BZN.items():
        try:
            xml = _api({'documentType': 'A44',
                        'in_Domain': bzn_code, 'out_Domain': bzn_code,
                        'periodStart': _fmt(start_14d), 'periodEnd': _fmt(now)})
            if xml:
                series = _parse_xml(xml)
                pts = sorted([p for ps in series.values() for p in ps], key=lambda x: x['ts'])
                prices[name] = pts
                print(f'    entsoe/price_{name}: {len(pts)} pts')
            else:
                prices[name] = []
                print(f'    entsoe/price_{name}: 204 no content')
            time.sleep(0.4)
        except Exception as e:
            print(f'  ! entsoe/price_{name}: {e}')
            prices[name] = []

    # ── Generation per fuel type (A75) ────────────────────────────────
    generation: Dict[str, List[dict]] = {}
    try:
        xml = _api({'documentType': 'A75', 'processType': 'A16',
                    'in_Domain': BZN['DE_LU'],
                    'periodStart': _fmt(start_7d), 'periodEnd': _fmt(now)}, timeout=90)
        if xml:
            raw = _parse_xml(xml, label_tag='psrType')
            generation = {PSRTYPE.get(k, k): v for k, v in raw.items()}
            total = sum(len(v) for v in generation.values())
            print(f'    entsoe/generation: {len(generation)} types, {total} total pts')
        time.sleep(0.5)
    except Exception as e:
        print(f'  ! entsoe/generation: {e}')

    # ── Load (A65) ────────────────────────────────────────────────────
    load: List[dict] = []
    try:
        xml = _api({'documentType': 'A65', 'processType': 'A16',
                    'outBiddingZone_Domain': BZN['DE_LU'],
                    'periodStart': _fmt(start_7d), 'periodEnd': _fmt(now)})
        if xml:
            series = _parse_xml(xml)
            load = sorted([p for ps in series.values() for p in ps], key=lambda x: x['ts'])
            print(f'    entsoe/load: {len(load)} pts')
        time.sleep(0.4)
    except Exception as e:
        print(f'  ! entsoe/load: {e}')

    # ── Cross-border flows (A11) ──────────────────────────────────────
    # We emit TWO views of the same data so the frontend has the schema
    # it asks for (crossborder_flows with 'from->to' keys), while we also
    # keep the neighbour-grouped flows_in/flows_out for analytic use.
    flows_in: Dict[str, List[dict]]  = {}
    flows_out: Dict[str, List[dict]] = {}
    crossborder_flows: Dict[str, List[dict]] = {}
    for src, dst in FLOW_PAIRS:
        for out_dom, in_dom, direction, store, label in [
            # DE → neighbour (exports from DE perspective)
            (BZN[src], BZN[dst], f'{src}->{dst}', flows_out, dst),
            # neighbour → DE (imports into DE)
            (BZN[dst], BZN[src], f'{dst}->{src}', flows_in,  dst),
        ]:
            try:
                xml = _api({'documentType': 'A11',
                            'out_Domain': out_dom, 'in_Domain': in_dom,
                            'periodStart': _fmt(start_7d), 'periodEnd': _fmt(now)})
                if xml:
                    series = _parse_xml(xml)
                    pts = sorted([p for ps in series.values() for p in ps], key=lambda x: x['ts'])
                    existing = store.get(label, [])
                    merged   = {p['ts']: p['v'] for p in existing}
                    merged.update({p['ts']: p['v'] for p in pts})
                    store[label] = [{'ts': t, 'v': v} for t, v in sorted(merged.items())]
                    # Mirror into crossborder_flows with the directed key the frontend wants
                    crossborder_flows[direction] = list(store[label])
                    print(f'    entsoe/flow {direction}: {len(store[label])} pts')
                time.sleep(0.3)
            except Exception as e:
                print(f'  ! entsoe/flow {direction}: {e}')

    # ── Net position derived from flows ───────────────────────────────
    net: Dict[int, float] = {}
    for pts in flows_in.values():
        for p in pts:
            net[p['ts']] = net.get(p['ts'], 0.0) + p['v']
    for pts in flows_out.values():
        for p in pts:
            net[p['ts']] = net.get(p['ts'], 0.0) - p['v']
    net_position = [{'ts': t, 'v': round(v, 1)} for t, v in sorted(net.items())]
    print(f'    entsoe/net_position: {len(net_position)} pts')

    return {
        'data': {
            'awaiting_key': False,
            'prices':       prices,
            'generation':   generation,
            'load':         load,
            'flows_in':     flows_in,
            'flows_out':    flows_out,
            'crossborder_flows': crossborder_flows,  # frontend reads this
            'net_position': net_position,
        },
        'meta': {
            'source':  'ENTSO-E Transparency Platform',
            'license': 'free for research and education',
            'note':    'prices A44 · generation A75 · load A65 · flows A11 · zone DE_LU',
        },
    }
