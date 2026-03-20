import json, os, time, requests
from datetime import datetime, timezone, timedelta

OUT = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(OUT, exist_ok=True)
SESSION = requests.Session()
SESSION.headers.update({'User-Agent': 'EnergyDashboard/1.0'})

def save(name, data):
    with open(os.path.join(OUT, f'{name}.json'), 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
    print(f'  ok {name}.json')

def now_utc():
    return datetime.now(timezone.utc)

def ts_ms(dt):
    return int(dt.timestamp() * 1000)

# ════════════════════════════════════════════════
# 1. SMARD
# ════════════════════════════════════════════════
SMARD_SERIES = {
    'wind_onshore':      1223252,
    'wind_offshore':     1223253,
    'solar':             1223258,
    'biomass':           1223257,
    'hydro':             1223255,
    'pumped_storage':    1223256,
    'nuclear':           1223251,
    'lignite':           1223249,
    'hard_coal':         1223248,
    'natural_gas':       1223250,
    'other_renewables':  1223260,
    'other':             1223259,
    'load':              1223263,
    'load_forecast':     1223264,
    'day_ahead_de':      4169,
    'day_ahead_at':      4996,
}

def fetch_smard_series(fid, start_ms):
    base = 'https://www.smard.de/app/chart_data'
    try:
        r = SESSION.get(f'{base}/{fid}/index_quarterhour.json', timeout=20)
        r.raise_for_status()
        timestamps = r.json().get('timestamps', [])
    except Exception as e:
        print(f'    index fail {fid}: {e}')
        return []

    relevant = [t for t in timestamps if t >= start_ms]
    if not relevant:
        relevant = timestamps[-8:]

    results = []
    seen = set()
    for bucket_ts in relevant[:4]:
        try:
            url = f'{base}/{fid}/{fid}_quarterhour_{bucket_ts}.json'
            r2 = SESSION.get(url, timeout=20)
            r2.raise_for_status()
            for e in r2.json().get('series', []):
                if isinstance(e, list) and len(e) == 2 and e[0] >= start_ms and e[1] is not None:
                    if e[0] not in seen:
                        seen.add(e[0])
                        results.append({'ts': e[0], 'value': round(float(e[1]), 2)})
            time.sleep(0.05)
        except Exception as e:
            print(f'    bucket fail {fid}/{bucket_ts}: {e}')

    return sorted(results, key=lambda x: x['ts'])

def fetch_smard():
    start_ms = ts_ms(now_utc() - timedelta(days=7))
    results = {}
    for name, fid in SMARD_SERIES.items():
        print(f'    smard: {name}')
        results[name] = fetch_smard_series(fid, start_ms)
    save('smard', {
        'updated': now_utc().isoformat(),
        'unit_generation': 'MWh',
        'unit_price': 'EUR/MWh',
        'series': results
    })

def fetch_smard_history():
    fid = 4169
    base = 'https://www.smard.de/app/chart_data'
    try:
        r = SESSION.get(f'{base}/{fid}/index_hour.json', timeout=20)
        r.raise_for_status()
        timestamps = r.json().get('timestamps', [])
    except Exception as e:
        print(f'  ! smard history index: {e}')
        save('smard_history', {'updated': now_utc().isoformat(), 'series': []})
        return

    cutoff_20y = ts_ms(now_utc() - timedelta(days=365 * 20))
    relevant = [t for t in timestamps if t >= cutoff_20y]
    step = max(1, len(relevant) // 40)
    sampled = relevant[::step]

    series = []
    for bucket_ts in sampled:
        try:
            url = f'{base}/{fid}/{fid}_hour_{bucket_ts}.json'
            r2 = SESSION.get(url, timeout=20)
            r2.raise_for_status()
            for e in r2.json().get('series', []):
                if isinstance(e, list) and len(e) == 2 and e[1] is not None:
                    series.append({'ts': e[0], 'value': round(float(e[1]), 2)})
            time.sleep(0.1)
        except Exception as e:
            print(f'  ! smard history bucket: {e}')

    series.sort(key=lambda x: x['ts'])
    save('smard_history', {
        'updated': now_utc().isoformat(),
        'unit': 'EUR/MWh',
        'series': series
    })

# ════════════════════════════════════════════════
# 2. Energy-Charts (Fraunhofer ISE)
# ════════════════════════════════════════════════
def fetch_energy_charts():
    BASE = 'https://api.energy-charts.info'
    results = {}

    for endpoint, key in [
        ('price?bzn=DE-LU&start=P7D', 'day_ahead_de'),
        ('price?bzn=AT&start=P7D',    'day_ahead_at'),
        ('price?bzn=FR&start=P7D',    'day_ahead_fr'),
        ('price?bzn=PL&start=P7D',    'day_ahead_pl'),
        ('price?bzn=CH&start=P7D',    'day_ahead_ch'),
    ]:
        try:
            r = SESSION.get(f'{BASE}/{endpoint}', timeout=20)
            r.raise_for_status()
            d = r.json()
            results[key] = {
                'unix_seconds': d.get('unix_seconds', []),
                'price': d.get('price', []),
                'unit': 'EUR/MWh'
            }
        except Exception as e:
            print(f'  ! {key}: {e}')
            results[key] = {}

    try:
        end = now_utc()
        r = SESSION.get(f'{BASE}/public_power', params={
            'country': 'de',
            'start': (end - timedelta(days=7)).strftime('%Y-%m-%dT%H:%MZ'),
            'end': end.strftime('%Y-%m-%dT%H:%MZ')
        }, timeout=25)
        r.raise_for_status()
        results['public_power'] = r.json()
    except Exception as e:
        print(f'  ! public_power: {e}')
        results['public_power'] = {}

    try:
        r = SESSION.get(f'{BASE}/installed_power?country=de&time_step=yearly', timeout=20)
        r.raise_for_status()
        results['installed_capacity'] = r.json()
    except Exception as e:
        print(f'  ! installed_capacity: {e}')
        results['installed_capacity'] = {}

    try:
        r = SESSION.get(f'{BASE}/ren_share_in_public_power?country=de&start=P30D', timeout=20)
        r.raise_for_status()
        results['renewables_share'] = r.json()
    except Exception as e:
        print(f'  ! ren_share: {e}')
        results['renewables_share'] = {}

    try:
        r = SESSION.get(f'{BASE}/cross_border_electricity_trading?country=de&start=P7D', timeout=20)
        r.raise_for_status()
        results['cross_border'] = r.json()
    except Exception as e:
        print(f'  ! cross_border: {e}')
        results['cross_border'] = {}

    try:
        r = SESSION.get(f'{BASE}/gas_price?start=P365D', timeout=20)
        r.raise_for_status()
        results['ttf_gas'] = r.json()
    except Exception as e:
        print(f'  ! ttf_gas: {e}')
        results['ttf_gas'] = {}

    for country in ['de', 'fr', 'at', 'es', 'it', 'pl']:
        try:
            r = SESSION.get(f'{BASE}/installed_power?country={country}&time_step=yearly', timeout=20)
            r.raise_for_status()
            results[f'installed_{country}'] = r.json()
        except Exception as e:
            print(f'  ! installed_{country}: {e}')

    save('energy_charts', {'updated': now_utc().isoformat(), **results})

# ════════════════════════════════════════════════
# 3. AGSI+ Gas Storage
# ════════════════════════════════════════════════
AGSI_COUNTRIES = {
    'eu': 'EU gesamt',
    'de': 'Deutschland',
    'at': 'Österreich',
    'fr': 'Frankreich',
    'it': 'Italien',
    'nl': 'Niederlande',
    'be': 'Belgien',
    'pl': 'Polen',
    'cz': 'Tschechien',
    'hu': 'Ungarn',
}

def fetch_agsi():
    results = {}
    for code, name in AGSI_COUNTRIES.items():
        try:
            r = SESSION.get('https://agsi.gie.eu/api', params={'type': code, 'size': 90}, timeout=25)
            r.raise_for_status()
            raw = r.json().get('data', [])
            cleaned = []
            for entry in raw:
                # Normalize across API versions
                fill = None
                for field in ['full_is_percentage', 'trend', 'gasInStorage']:
                    if entry.get(field) is not None:
                        fill = entry[field]
                        break
                if fill is None and isinstance(entry.get('status'), dict):
