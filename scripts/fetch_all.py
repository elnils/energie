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

# ── 1. SMARD ──────────────────────────────────
SMARD_SERIES = {
    'wind_onshore': 1223252, 'wind_offshore': 1223253,
    'solar': 1223258, 'biomass': 1223257,
    'hydro': 1223255, 'pumped_storage': 1223256,
    'nuclear': 1223251, 'lignite': 1223249,
    'hard_coal': 1223248, 'natural_gas': 1223250,
    'other': 1223259, 'load': 1223263, 'day_ahead_de': 4169,
}

def fetch_smard():
    results = {}
    start_ms = ts_ms(now_utc() - timedelta(days=7))
    for name, fid in SMARD_SERIES.items():
        try:
            r = SESSION.get(f'https://www.smard.de/app/chart_data/{fid}/index_quarterhour.json', timeout=15)
            r.raise_for_status()
            timestamps = r.json().get('timestamps', [])
            relevant = [t for t in timestamps if t >= start_ms] or timestamps[-4:]
            r2 = SESSION.get(f'https://www.smard.de/app/chart_data/{fid}/{fid}_quarterhour_{relevant[0]}.json', timeout=15)
            r2.raise_for_status()
            results[name] = [{'ts': e[0], 'value': round(e[1], 2)} for e in r2.json().get('series', []) if isinstance(e, list) and len(e) == 2 and e[0] >= start_ms and e[1] is not None]
            time.sleep(0.1)
        except Exception as e:
            print(f'  ! SMARD {name}: {e}')
            results[name] = []
    save('smard', {'updated': now_utc().isoformat(), 'unit_generation': 'MWh', 'unit_price': 'EUR/MWh', 'series': results})

# ── 2. Energy-Charts (Fraunhofer ISE) ─────────
def fetch_energy_charts():
    BASE = 'https://api.energy-charts.info'
    results = {}
    try:
        r = SESSION.get(f'{BASE}/price?bzn=DE-LU&start=P7D', timeout=20)
        r.raise_for_status()
        d = r.json()
        results['day_ahead_price'] = {'unix_seconds': d.get('unix_seconds', []), 'price': d.get('price', []), 'unit': 'EUR/MWh'}
    except Exception as e:
        print(f'  ! price: {e}')
        results['day_ahead_price'] = {}
    try:
        end = now_utc()
        r = SESSION.get(f'{BASE}/public_power', params={'country': 'de', 'start': (end - timedelta(days=7)).strftime('%Y-%m-%dT%H:%MZ'), 'end': end.strftime('%Y-%m-%dT%H:%MZ')}, timeout=20)
        r.raise_for_status()
        results['public_power'] = r.json()
    except Exception as e:
        print(f'  ! public_power: {e}')
        results['public_power'] = {}
    try:
        r = SESSION.get(f'{BASE}/installed_power?country=de', timeout=20)
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
        r = SESSION.get(f'{BASE}/installed_power?country=de&time_step=yearly', timeout=20)
        r.raise_for_status()
        results['installed_power_yearly'] = r.json()
    except Exception as e:
        print(f'  ! installed_power_yearly: {e}')
    save('energy_charts', {'updated': now_utc().isoformat(), **results})

# ── 3. AGSI Gas Storage ────────────────────────
def fetch_agsi():
    try:
        r = SESSION.get('https://agsi.gie.eu/api?type=eu&from=2024-01-01&size=30', timeout=20)
        r.raise_for_status()
        save('gas_storage', {'updated': now_utc().isoformat(), 'source': 'GIE AGSI+', 'data': r.json().get('data', [])})
    except Exception as e:
        print(f'  ! AGSI: {e}')
        save('gas_storage', {'updated': now_utc().isoformat(), 'data': [], 'error': str(e)})

# ── 4. ECB Inflation + EUR/USD ─────────────────
def fetch_ecb():
    BASE = 'https://data-api.ecb.europa.eu/service/data'
    results = {}
    try:
        r = SESSION.get(f'{BASE}/ICP/M.DE.N.000000.4.ANR?format=jsondata&lastNObservations=36', timeout=20, headers={'Accept': 'application/json'})
        r.raise_for_status()
        d = r.json()
        obs = d['dataSets'][0]['series']['0:0:0:0:0:0']['observations']
        periods = d['structure']['dimensions']['observation'][0]['values']
        results['hicp_de'] = {'unit': '% annual rate', 'series': [{'period': periods[int(k)]['id'], 'value': v[0]} for k, v in sorted(obs.items(), key=lambda x: int(x[0]))]}
    except Exception as e:
        print(f'  ! ECB HICP: {e}')
        results['hicp_de'] = {}
    try:
        r = SESSION.get(f'{BASE}/EXR/D.USD.EUR.SP00.A?format=jsondata&lastNObservations=90', timeout=20, headers={'Accept': 'application/json'})
        r.raise_for_status()
        d = r.json()
        obs = d['dataSets'][0]['series']['0:0:0:0:0']['observations']
        periods = d['structure']['dimensions']['observation'][0]['values']
        results['eur_usd'] = [{'date': periods[int(k)]['id'], 'value': round(v[0], 4)} for k, v in sorted(obs.items(), key=lambda x: int(x[0]))]
    except Exception as e:
        print(f'  ! ECB EUR/USD: {e}')
        results['eur_usd'] = []
    save('macro', {'updated': now_utc().isoformat(), 'source': 'ECB SDW', **results})

# ── 5. Open-Meteo Wetter ───────────────────────
def fetch_weather():
    cities = {'berlin': (52.52, 13.41), 'hamburg': (53.55, 10.00), 'munich': (48.14, 11.58), 'cologne': (50.94, 6.96), 'frankfurt': (50.11, 8.68)}
    results = {}
    for city, (lat, lon) in cities.items():
        try:
            r = SESSION.get('https://api.open-meteo.com/v1/forecast', params={'latitude': lat, 'longitude': lon, 'current': 'temperature_2m,wind_speed_10m,cloud_cover,direct_radiation', 'hourly': 'temperature_2m,wind_speed_100m,direct_radiation', 'forecast_days': 3, 'timezone': 'Europe/Berlin'}, timeout=15)
            r.raise_for_status()
            results[city] = r.json()
        except Exception as e:
            print(f'  ! weather {city}: {e}')
    save('weather', {'updated': now_utc().isoformat(), 'cities': results})

# ── 6. Expansion (Fraunhofer) ──────────────────
def fetch_expansion():
    results = {}
    try:
        r = SESSION.get('https://api.energy-charts.info/installed_power?country=de&time_step=yearly', timeout=20)
        r.raise_for_status()
        results['installed_power_yearly'] = r.json()
    except Exception as e:
        print(f'  ! expansion: {e}')
    save('expansion', {'updated': now_utc().isoformat(), 'source': 'Fraunhofer ISE', **results})

# ── 7. Commodities ─────────────────────────────
def fetch_commodities():
    results = {}
    try:
        r = SESSION.get('https://ember-climate.org/app/uploads/2022/03/Carbon-Price-Viewer.csv', timeout=20)
        if r.status_code == 200:
            lines = r.text.strip().split('\n')
            parsed = []
            for line in lines[1:]:
                p = line.split(',')
                if len(p) >= 2:
                    try: parsed.append({'date': p[0].strip(), 'price_eur': float(p[1].strip())})
                    except: pass
            results['eu_ets_co2'] = {'unit': 'EUR/tCO2', 'series': parsed[-90:]}
    except Exception as e:
        print(f'  ! CO2: {e}')
    try:
        r = SESSION.get('https://api.energy-charts.info/gas_price?start=P180D', timeout=20)
        if r.status_code == 200:
            results['ttf_gas'] = r.json()
    except Exception as e:
        print(f'  ! TTF: {e}')
    for ticker, name in [('BZ%3DF', 'brent_crude'), ('NG%3DF', 'natgas_henry_hub')]:
        try:
            r = SESSION.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=6mo&interval=1d', timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
            r.raise_for_status()
            d = r.json()['chart']['result'][0]
            results[name] = {'unit': 'USD/barrel' if 'brent' in name else 'USD/MMBtu', 'series': [{'ts': t, 'price': round(p, 2)} for t, p in zip(d['timestamp'], d['indicators']['quote'][0]['close']) if p]}
        except Exception as e:
            print(f'  ! Yahoo {name}: {e}')
    try:
        r = SESSION.get('https://creativecommons.tankerkoenig.de/json/list.php?lat=51.163375&lng=10.447683&rad=100&sort=price&type=all&apikey=00000000-0000-0000-0000-000000000002', timeout=15)
        if r.status_code == 200:
            stations = r.json().get('stations', [])
            def avg(lst): return round(sum(lst)/len(lst), 3) if lst else None
            results['retail_fuel_de'] = {'e5_avg': avg([s['e5'] for s in stations if s.get('e5')]), 'e10_avg': avg([s['e10'] for s in stations if s.get('e10')]), 'diesel_avg': avg([s['diesel'] for s in stations if s.get('diesel')]), 'unit': 'EUR/liter', 'fetched_at': now_utc().isoformat()}
    except Exception as e:
        print(f'  ! Tankerkoenig: {e}')
    save('commodities', {'updated': now_utc().isoformat(), **results})

# ── 8. Meta ────────────────────────────────────
def write_meta():
    save('meta', {'last_fetch': now_utc().isoformat(), 'next_fetch_approx': (now_utc() + timedelta(minutes=15)).isoformat(), 'sources': {'smard': 'smard.de', 'energy_charts': 'api.energy-charts.info', 'gas_storage': 'agsi.gie.eu', 'macro': 'data-api.ecb.europa.eu', 'weather': 'api.open-meteo.com', 'commodities': 'Yahoo + Tankerkoenig + Ember', 'expansion': 'api.energy-charts.info'}})

if __name__ == '__main__':
    print(f'=== Fetch Start: {now_utc().isoformat()} ===')
    for label, fn in [
        ('SMARD', fetch_smard),
        ('Energy-Charts', fetch_energy_charts),
        ('AGSI Gas', fetch_agsi),
        ('ECB Makro', fetch_ecb),
        ('Wetter', fetch_weather),
        ('Expansion', fetch_expansion),
        ('Commodities', fetch_commodities),
        ('Meta', write_meta),
    ]:
        print(f'\n[{label}]')
        try: fn()
        except Exception as e: print(f'  !! FEHLER: {e}')
    print(f'\n=== Fertig: {now_utc().isoformat()} ===')
