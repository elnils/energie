"""
Jahreskennzahlen Strommarkt Deutschland (nach dem Vorbild des neon-Dashboards).

Quelle: Energy-Charts (Fraunhofer ISE), öffentliche Nettostromerzeugung in
15-Minuten-Auflösung, Day-Ahead-Preise DE-LU (bis 30.09.2018 DE-AT-LU) und
installierte Leistung (jährlich). Pro Kalenderjahr zwei große Abrufe
(~5 MB Erzeugung, ~0,3 MB Preise); daraus wird je Jahr berechnet:

  spot_avg          zeitgewichteter Mittelwert Day-Ahead (€/MWh)
  mv_solar/_wind_*  Marktwert = erzeugungsgewichteter Preis (€/MWh)
  mvf_solar/_wind_* Marktwertfaktor = Marktwert / Mittelpreis
  neg_hours         Stunden mit negativem Preis (15-min-Werte anteilig)
  spike_hours       Stunden mit Preis > 200 €/MWh
  max/min_price     Extremwerte
  load_twh, gen_twh, residual_twh, net_import_twh (positiv = Nettoimport)
  ren_share_load    Anteil Erneuerbare an der Last (%)
  flh_*             Volllaststunden = Erzeugung / mittlere installierte Leistung
  gas_avg           Jahresmittel TTF (Yahoo TTF=F, Monatswerte), wenn verfügbar

Abgeschlossene Jahre werden aus der vorigen Datei übernommen und nicht neu
geladen; pro Lauf werden höchstens MAX_BACKFILL fehlende Jahre nachgeholt,
das laufende Jahr wird immer neu gerechnet (YTD).
"""
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from core import http, store

API = 'https://api.energy-charts.info'
FIRST_YEAR = 2015
MAX_BACKFILL = 12
SPIKE_EUR = 200.0

GEN_EXCLUDE = {
    'Load', 'Residual load', 'Renewable share of load', 'Renewable share of generation',
    'Cross border electricity trading', 'Hydro pumped storage consumption',
}
RENEWABLE = {
    'Hydro Run-of-River', 'Biomass', 'Geothermal', 'Hydro water reservoir',
    'Wind offshore', 'Wind onshore', 'Solar',
}


def _get(path: str, params: dict, timeout: int = 60) -> dict:
    r = http.get_session().get(f'{API}/{path}', params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _durations(ts: List[int]) -> List[float]:
    """Dauer jedes Intervalls in Stunden (letztes Intervall wie das vorletzte)."""
    out = []
    for i in range(len(ts)):
        if i + 1 < len(ts):
            d = ts[i + 1] - ts[i]
        else:
            d = ts[i] - ts[i - 1] if i else 3600
        out.append(max(0, min(d, 3600)) / 3600.0)
    return out


def _prices(year: int) -> Dict[str, list]:
    """Day-Ahead-Preise eines Jahres; vor Oktober 2018 Gebotszone DE-AT-LU."""
    start, end = f'{year}-01-01', f'{year}-12-31'
    ts: List[int] = []
    pr: List[float] = []
    zones = ['DE-LU'] if year > 2018 else (['DE-AT-LU', 'DE-LU'] if year == 2018 else ['DE-AT-LU'])
    seen = set()
    for bzn in zones:
        try:
            d = _get('price', {'bzn': bzn, 'start': start, 'end': end})
        except Exception as e:
            print(f'  ! kennzahlen/price {bzn} {year}: {e}')
            continue
        for t, p in zip(d.get('unix_seconds') or [], d.get('price') or []):
            if p is None or t in seen:
                continue
            seen.add(t)
            ts.append(t)
            pr.append(float(p))
    pairs = sorted(zip(ts, pr))
    return {'ts': [p[0] for p in pairs], 'price': [p[1] for p in pairs]}


def _price_lookup(price_ts: List[int], prices: List[float]):
    """Liefert eine Funktion t -> Preis des Intervalls, das t enthält."""
    import bisect

    def at(t: int) -> Optional[float]:
        i = bisect.bisect_right(price_ts, t) - 1
        if i < 0:
            return None
        # Lücken über 2 h nicht überbrücken
        if t - price_ts[i] > 7200:
            return None
        return prices[i]
    return at


def _capacity() -> Dict[str, Dict[int, float]]:
    d = _get('installed_power', {'country': 'de', 'time_step': 'yearly',
                                 'installation_decommission': 'false'})
    years = [int(y) for y in d.get('time', [])]
    want = {'Solar DC': 'solar', 'Wind onshore': 'wind_on', 'Wind offshore': 'wind_off'}
    out: Dict[str, Dict[int, float]] = {}
    for p in d.get('production_types', []):
        key = want.get(p.get('name'))
        if key:
            out[key] = {y: v for y, v in zip(years, p.get('data', [])) if v is not None}
    return out


def _gas_yearly() -> Dict[int, float]:
    """Jahresmittel TTF aus Yahoo-Monatsschlusskursen (best effort)."""
    try:
        r = http.get_session().get(
            'https://query1.finance.yahoo.com/v8/finance/chart/TTF%3DF',
            params={'range': '15y', 'interval': '1mo'}, timeout=20,
            headers={'Referer': 'https://finance.yahoo.com'})
        r.raise_for_status()
        res = r.json()['chart']['result'][0]
        closes = res['indicators']['quote'][0]['close']
        acc: Dict[int, List[float]] = {}
        for t, c in zip(res['timestamp'], closes):
            if c is None or not (1 < c < 500):
                continue
            y = datetime.fromtimestamp(t, timezone.utc).year
            acc.setdefault(y, []).append(c)
        return {y: round(sum(v) / len(v), 2) for y, v in acc.items() if v}
    except Exception as e:
        print(f'  ! kennzahlen/gas: {e}')
        return {}


def _compute_year(year: int, cap: Dict[str, Dict[int, float]]) -> Optional[dict]:
    pw = _get('public_power', {'country': 'de', 'start': f'{year}-01-01',
                               'end': f'{year}-12-31'}, timeout=120)
    ts = pw.get('unix_seconds') or []
    if len(ts) < 96:
        return None
    dur = _durations(ts)
    types = {p['name']: p.get('data') or [] for p in pw.get('production_types', [])}

    def energy(name: str) -> float:
        """MWh aus MW-Leistungswerten."""
        vals = types.get(name) or []
        return sum((v or 0.0) * h for v, h in zip(vals, dur))

    pr = _prices(year)
    at = _price_lookup(pr['ts'], pr['price'])
    p_dur = _durations(pr['ts'])
    hours_priced = sum(p_dur)
    spot_avg = (sum(p * h for p, h in zip(pr['price'], p_dur)) / hours_priced) if hours_priced else None
    neg_hours = sum(h for p, h in zip(pr['price'], p_dur) if p < 0)
    spike_hours = sum(h for p, h in zip(pr['price'], p_dur) if p > SPIKE_EUR)

    def market_value(names: List[str]) -> Optional[float]:
        cols = [types.get(n) or [] for n in names]
        num = den = 0.0
        for i, t in enumerate(ts):
            g = sum((c[i] or 0.0) for c in cols if i < len(c))
            if g <= 0:
                continue
            p = at(t)
            if p is None:
                continue
            num += p * g * dur[i]
            den += g * dur[i]
        return num / den if den else None

    mv = {
        'solar': market_value(['Solar']),
        'wind_on': market_value(['Wind onshore']),
        'wind_off': market_value(['Wind offshore']),
    }

    load = energy('Load')
    gen = sum(energy(n) for n in types if n not in GEN_EXCLUDE)
    ren = sum(energy(n) for n in types if n in RENEWABLE)
    residual = energy('Residual load')
    net_import = energy('Cross border electricity trading')

    span_h = (ts[-1] - ts[0]) / 3600.0 + dur[-1]
    year_h = (datetime(year + 1, 1, 1) - datetime(year, 1, 1)).total_seconds() / 3600.0
    partial = span_h < year_h - 48

    def flh(key: str, name: str) -> Optional[float]:
        c = cap.get(key) or {}
        now_c, prev_c = c.get(year), c.get(year - 1)
        if now_c is None and prev_c is None:
            return None
        mean_gw = ((now_c or prev_c) + (prev_c or now_c)) / 2.0
        if not mean_gw:
            return None
        hours = energy(name) / (mean_gw * 1000.0)
        return hours

    def r(x, n=2):
        return None if x is None else round(x, n)

    def factor(m):
        return r(m / spot_avg, 3) if (m is not None and spot_avg) else None

    return {
        'year': year,
        'partial': partial,
        'hours_covered': r(span_h, 1),
        'until': datetime.fromtimestamp(ts[-1], timezone.utc).strftime('%Y-%m-%d'),
        'spot_avg': r(spot_avg),
        'max_price': r(max(pr['price'])) if pr['price'] else None,
        'min_price': r(min(pr['price'])) if pr['price'] else None,
        'neg_hours': r(neg_hours, 1),
        'spike_hours': r(spike_hours, 1),
        'mv_solar': r(mv['solar']),
        'mv_wind_on': r(mv['wind_on']),
        'mv_wind_off': r(mv['wind_off']),
        'mvf_solar': factor(mv['solar']),
        'mvf_wind_on': factor(mv['wind_on']),
        'mvf_wind_off': factor(mv['wind_off']),
        'load_twh': r(load / 1e6),
        'gen_twh': r(gen / 1e6),
        'residual_twh': r(residual / 1e6),
        'net_import_twh': r(net_import / 1e6),
        'ren_share_load': r(100.0 * ren / load, 1) if load else None,
        'solar_twh': r(energy('Solar') / 1e6),
        'wind_on_twh': r(energy('Wind onshore') / 1e6),
        'wind_off_twh': r(energy('Wind offshore') / 1e6),
        'flh_solar': r(flh('solar', 'Solar'), 0),
        'flh_wind_on': r(flh('wind_on', 'Wind onshore'), 0),
        'flh_wind_off': r(flh('wind_off', 'Wind offshore'), 0),
        'computed_at': store.now_iso(),
    }


def fetch() -> dict:
    now = datetime.now(timezone.utc)
    this_year = now.year
    prev = (store.read_json('kennzahlen') or {}).get('data', {}) or {}
    old = {y['year']: y for y in prev.get('years', []) if isinstance(y, dict) and 'year' in y}

    cap = _capacity()
    gas = _gas_yearly()

    def needs(y: int) -> bool:
        e = old.get(y)
        if e is None or e.get('partial'):
            return True
        # Vorjahr bis Mitte Januar nachrechnen (späte Datenkorrekturen)
        if y == this_year - 1 and now.month == 1 and now.day < 20:
            return True
        return False

    todo = [y for y in range(this_year, FIRST_YEAR - 1, -1) if needs(y)]
    # laufendes Jahr immer, dazu höchstens MAX_BACKFILL ältere
    todo = todo[:MAX_BACKFILL + 1]
    errors = []
    for y in todo:
        try:
            t0 = time.time()
            res = _compute_year(y, cap)
            if res:
                old[y] = res
                print(f'  · kennzahlen {y}: ok ({time.time() - t0:.1f}s)')
        except Exception as e:
            errors.append(f'{y}: {type(e).__name__}: {e}')
            print(f'  ! kennzahlen {y}: {e}')

    if not old:
        raise RuntimeError('Energy-Charts: keine Jahreskennzahlen berechnet; ' + '; '.join(errors))

    years = []
    for y in sorted(old):
        e = dict(old[y])
        if gas.get(y) is not None:
            e['gas_avg'] = gas[y]
        years.append(e)

    missing = [y for y in range(FIRST_YEAR, this_year + 1) if y not in old]
    return {
        'data': {
            'years': years,
            'capacity_gw': {k: {str(y): v for y, v in d.items()} for k, d in cap.items()},
            'missing_years': missing,
            'errors': errors,
        },
        'meta': {
            'source': 'Energy-Charts (Fraunhofer ISE), öffentliche Nettostromerzeugung; '
                      'Day-Ahead DE-LU/DE-AT-LU; TTF: ICE via Yahoo Finance',
            'license': 'CC BY 4.0 (Energy-Charts)',
            'url': 'https://energy-charts.info',
            'note': 'Marktwert = erzeugungsgewichteter Day-Ahead-Preis; Faktor = Marktwert / '
                    'zeitgewichteter Mittelpreis. Volllaststunden auf Basis der öffentlichen '
                    'Nettoerzeugung (ohne Eigenverbrauch) und der mittleren installierten '
                    'Leistung (Jahresende Vorjahr/aktuelles Jahr), daher bei Solar niedriger '
                    'als amtliche Werte. Spitzenstunden: Preis > 200 €/MWh.',
        },
    }
