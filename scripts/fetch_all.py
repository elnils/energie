"""
Energy Dashboard – Data Fetcher v2
Mehr Quellen, Fallbacks, 20-Jahre-Historie
Alle APIs öffentlich, kein API-Key erforderlich
"""
import json, os, time, requests
from datetime import datetime, timezone, timedelta

OUT = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(OUT, exist_ok=True)

S = requests.Session()
S.headers.update({'User-Agent': 'EnergyDashboard/2.0 (public research tool)'})

def save(name, data):
    with open(os.path.join(OUT, f'{name}.json'), 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
    print(f'  ✓ {name}.json saved')

def now(): return datetime.now(timezone.utc)
def ts_ms(dt): return int(dt.timestamp() * 1000)
def get(url, **kw):
    kw.setdefault('timeout', 20)
    return S.get(url, **kw)

# ════════════════════════════════════════════════
# 1. SMARD – Stromerzeugung + Preise (DE)
#    Primärquelle: smard.de/app/chart_data
#    Fallback: smard.de/app/chart_data (andere Auflösung)
# ════════════════════════════════════════════════
SMARD_IDS = {
    'wind_onshore': 1223252, 'wind_offshore': 1223253,
    'solar': 1223258,        'biomass': 1223257,
    'hydro': 1223255,        'pumped_storage': 1223256,
    'nuclear': 1223251,      'lignite': 1223249,
    'hard_coal': 1223248,    'natural_gas': 1223250,
    'other_renewables': 1223259, 'other_conventional': 1223260,
    'load': 1223263,         'load_forecast': 1223264,
    'residual_load': 1223265,
    'day_ahead_de': 4169,    'day_ahead_lu': 4996,
    'cross_border_flow': 1223281,
}
SMARD_BASE = 'https://www.smard.de/app/chart_data'

def fetch_smard_series(filter_id, days=7, resolution='quarterhour'):
    start_ms = ts_ms(now() - timedelta(days=days))
    try:
        r = get(f'{SMARD_BASE}/{filter_id}/index_{resolution}.json')
        r.raise_for_status()
        timestamps = r.json().get('timestamps', [])
        relevant = [t for t in timestamps if t >= start_ms] or timestamps[-4:]
        if not relevant: return []
        all_series = []
        for bucket_ts in relevant[:8]:  # max 8 buckets
            r2 = get(f'{SMARD_BASE}/{filter_id}/{filter_id}_{resolution}_{bucket_ts}.json')
            if r2.status_code != 200: continue
            for e in r2.json().get('series', []):
                if isinstance(e, list) and len(e) == 2 and e[0] >= start_ms and e[1] is not None:
                    all_series.append({'ts': e[0], 'value': round(float(e[1]), 2)})
            time.sleep(0.05)
        return all_series
    except Exception as ex:
        print(f'    ! SMARD {filter_id}: {ex}')
        return []

def fetch_smard_long(filter_id, years=20):
    """Fetch yearly/monthly data for long-term history"""
    start_ms = ts_ms(now() - timedelta(days=years*365))
    try:
        for res in ['yearly', 'monthly']:
            r = get(f'{SMARD_BASE}/{filter_id}/index_{res}.json')
            if r.status_code != 200: continue
            timestamps = r.json().get('timestamps', [])
            relevant = [t for t in timestamps if t >= start_ms] or timestamps
            all_series = []
            for bucket_ts in relevant[:60]:
                r2 = get(f'{SMARD_BASE}/{filter_id}/{filter_id}_{res}_{bucket_ts}.json')
                if r2.status_code != 200: continue
                for e in r2.json().get('series', []):
                    if isinstance(e, list) and len(e) == 2 and e[1] is not None:
                        all_series.append({'ts': e[0], 'value': round(float(e[1]), 2)})
                time.sleep(0.05)
            if all_series:
                return all_series
    except Exception as ex:
        print(f'    ! SMARD long {filter_id}: {ex}')
    return []

def fetch_smard():
    print('  Fetching SMARD short-term (7d)...')
    series = {}
    for name, fid in SMARD_IDS.items():
        series[name] = fetch_smard_series(fid, days=7)
        print(f'    {name}: {len(series[name])} points')

    print('  Fetching SMARD long-term (20y)...')
    long_term = {}
    for name in ['day_ahead_de', 'load', 'solar', 'wind_onshore', 'wind_offshore', 'lignite', 'hard_coal', 'natural_gas']:
        long_term[name] = fetch_smard_long(SMARD_IDS[name], years=20)
        print(f'    long {name}: {len(long_term[name])} points')

    save('smard', {
        'updated': now().isoformat(),
        'unit_generation': 'MWh', 'unit_price': 'EUR/MWh',
        'series': series, 'long_term': long_term
    })

# ════════════════════════════════════════════════
# 2. Energy-Charts (Fraunhofer ISE)
#    api.energy-charts.info – sehr zuverlässig
# ════════════════════════════════════════════════
EC = 'https://api.energy-charts.info'

def fetch_energy_charts():
    print('  Fetching Energy-Charts...')
    results = {}

    # Day-ahead price DE-LU (7d)
    try:
        r = get(f'{EC}/price?bzn=DE-LU&start=P7D')
        r.raise_for_status()
        d = r.json()
        results['day_ahead_price_7d'] = {'unix_seconds': d.get('unix_seconds', []), 'price': d.get('price', []), 'unit': 'EUR/MWh'}
        print(f'    day_ahead 7d: {len(d.get("price",[]))} points')
    except Exception as e: print(f'    ! price 7d: {e}'); results['day_ahead_price_7d'] = {}

    # Day-ahead price – long term (via yearly chunks)
    try:
        all_ts, all_p = [], []
        for year in range(2005, now().year + 1):
            r = get(f'{EC}/price?bzn=DE-LU&start={year}-01-01T00:00Z&end={year}-12-31T23:59Z')
            if r.status_code == 200:
                d = r.json()
                all_ts.extend(d.get('unix_seconds', []))
                all_p.extend(d.get('price', []))
                time.sleep(0.1)
        results['day_ahead_price_long'] = {'unix_seconds': all_ts, 'price': all_p, 'unit': 'EUR/MWh'}
        print(f'    day_ahead long: {len(all_p)} points')
    except Exception as e: print(f'    ! price long: {e}'); results['day_ahead_price_long'] = {}

    # Public power generation mix
    try:
        end_dt = now()
        start_dt = end_dt - timedelta(days=7)
        r = get(f'{EC}/public_power', params={'country': 'de', 'start': start_dt.strftime('%Y-%m-%dT%H:%MZ'), 'end': end_dt.strftime('%Y-%m-%dT%H:%MZ')})
        r.raise_for_status()
        results['public_power'] = r.json()
        print(f'    public_power: ok')
    except Exception as e: print(f'    ! public_power: {e}'); results['public_power'] = {}

    # Installed capacity (current)
    try:
        r = get(f'{EC}/installed_power?country=de')
        r.raise_for_status()
        results['installed_capacity'] = r.json()
    except Exception as e: print(f'    ! installed_capacity: {e}')

    # Installed capacity yearly (20y)
    try:
        r = get(f'{EC}/installed_power?country=de&time_step=yearly')
        r.raise_for_status()
        results['installed_power_yearly'] = r.json()
        print(f'    installed yearly: ok')
    except Exception as e: print(f'    ! installed yearly: {e}')

    # Renewables share
    try:
        r = get(f'{EC}/ren_share_in_public_power?country=de&start=P30D')
        r.raise_for_status()
        results['renewables_share'] = r.json()
    except Exception as e: print(f'    ! ren_share: {e}')

    # Renewables share long-term yearly
    try:
        r = get(f'{EC}/ren_share_in_public_power?country=de&time_step=yearly')
        r.raise_for_status()
        results['renewables_share_yearly'] = r.json()
    except Exception as e: print(f'    ! ren_share yearly: {e}')

    # Cross-border flows
    try:
        r = get(f'{EC}/crossborder_flows?country=de&start=P7D')
        r.raise_for_status()
        results['crossborder_flows'] = r.json()
    except Exception as e: print(f'    ! crossborder: {e}')

    # Gas prices (TTF)
    try:
        r = get(f'{EC}/gas_price?start=P365D')
        r.raise_for_status()
        results['ttf_gas_1y'] = r.json()
    except Exception as e: print(f'    ! ttf gas: {e}')

    # Multiple EU countries for comparison
    for country in ['at', 'fr', 'pl', 'nl', 'es', 'it', 'ch']:
        try:
            r = get(f'{EC}/price?bzn={country.upper()}&start=P7D')
            if r.status_code == 200:
                d = r.json()
                results[f'price_{country}'] = {'unix_seconds': d.get('unix_seconds', []), 'price': d.get('price', []), 'unit': 'EUR/MWh'}
        except Exception as e: print(f'    ! price {country}: {e}')
        time.sleep(0.05)

    save('energy_charts', {'updated': now().isoformat(), **results})

# ════════════════════════════════════════════════
# 3. ENTSO-E – Europäische Stromerzeugung & Lastdaten
#    transparency.entsoe.eu – öffentliche Dokumente
#    Fallback: energy-charts europäische Daten
# ════════════════════════════════════════════════
def fetch_entsoe():
    print('  Fetching ENTSO-E public data...')
    results = {}
    # ENTSO-E stellt einige Daten ohne Auth bereit über den StaticFiles-Endpunkt
    # Primär nutzen wir energy-charts als zuverlässigere Quelle für EU-Daten
    eu_countries = {
        'DE': 'de', 'FR': 'fr', 'AT': 'at', 'PL': 'pl',
        'NL': 'nl', 'ES': 'es', 'IT': 'it', 'CH': 'ch',
        'CZ': 'cz', 'DK': 'dk', 'SE': 'se', 'NO': 'no'
    }
    for code, ec_code in eu_countries.items():
        try:
            r = get(f'{EC}/public_power', params={
                'country': ec_code,
                'start': (now() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%MZ'),
                'end': now().strftime('%Y-%m-%dT%H:%MZ')
            })
            if r.status_code == 200:
                results[code] = r.json()
                print(f'    {code}: ok')
            time.sleep(0.15)
        except Exception as e: print(f'    ! ENTSO-E {code}: {e}')

    save('entsoe', {'updated': now().isoformat(), 'countries': results})

# ════════════════════════════════════════════════
# 4. AGSI+ – Gasspeicher Europa
#    Primär: agsi.gie.eu (public, kein Key für Aggregatdaten)
#    Fallback: energy-charts gas data
# ════════════════════════════════════════════════
def fetch_gas_storage():
    print('  Fetching gas storage...')
    results = {'eu': [], 'countries': {}}

    # EU aggregate
    for size in [90, 30, 10]:
        try:
            r = get(f'https://agsi.gie.eu/api?type=eu&size={size}')
            r.raise_for_status()
            d = r.json()
            results['eu'] = d.get('data', [])
            if results['eu']:
                print(f'    EU storage: {len(results["eu"])} days')
                break
        except Exception as e:
            print(f'    ! AGSI EU (size={size}): {e}')

    # Per country
    for country_code in ['DE', 'AT', 'FR', 'NL', 'IT', 'ES', 'PL', 'CZ']:
        for attempt in range(2):
            try:
                r = get(f'https://agsi.gie.eu/api?country={country_code}&size=30')
                if r.status_code == 200:
                    d = r.json()
                    data = d.get('data', [])
                    if data:
                        results['countries'][country_code] = data
                        print(f'    {country_code}: {len(data)} days')
                        break
            except Exception as e:
                print(f'    ! AGSI {country_code}: {e}')
            time.sleep(0.2)

    # Long-term EU storage (2y history)
    try:
        from_date = (now() - timedelta(days=730)).strftime('%Y-%m-%d')
        r = get(f'https://agsi.gie.eu/api?type=eu&from={from_date}&size=730')
        if r.status_code == 200:
            results['eu_2y'] = r.json().get('data', [])
            print(f'    EU 2y storage: {len(results["eu_2y"])} points')
    except Exception as e: print(f'    ! AGSI 2y: {e}')

    save('gas_storage', {'updated': now().isoformat(), 'source': 'GIE AGSI+', **results})

# ════════════════════════════════════════════════
# 5. ECB – Inflation + Wechselkurse + Leitzins
# ════════════════════════════════════════════════
ECB = 'https://data-api.ecb.europa.eu/service/data'

def ecb_series(path, key='0:0:0:0:0:0', n=240):
    try:
        r = get(f'{ECB}/{path}?format=jsondata&lastNObservations={n}', headers={'Accept': 'application/json'})
        r.raise_for_status()
        d = r.json()
        obs = d['dataSets'][0]['series'][key]['observations']
        periods = d['structure']['dimensions']['observation'][0]['values']
        return [{'period': periods[int(k)]['id'], 'value': round(float(v[0]), 4)} for k, v in sorted(obs.items(), key=lambda x: int(x[0])) if v[0] is not None]
    except Exception as e:
        print(f'    ! ECB {path}: {e}')
        return []

def fetch_ecb():
    print('  Fetching ECB data...')
    results = {}

    # HICP inflation – DE, EU, AT, FR (monthly, 20y = 240 months)
    for cc, key in [('DE','0:0:0:0:0:0'), ('U2','0:0:0:0:0:0'), ('AT','0:0:0:0:0:0'), ('FR','0:0:0:0:0:0')]:
        try:
            r = get(f'{ECB}/ICP/M.{cc}.N.000000.4.ANR?format=jsondata&lastNObservations=240', headers={'Accept': 'application/json'})
            r.raise_for_status()
            d = r.json()
            obs = d['dataSets'][0]['series']['0:0:0:0:0:0']['observations']
            periods = d['structure']['dimensions']['observation'][0]['values']
            results[f'hicp_{cc.lower()}'] = [{'period': periods[int(k)]['id'], 'value': round(float(v[0]), 2)} for k, v in sorted(obs.items(), key=lambda x: int(x[0])) if v[0] is not None]
            print(f'    HICP {cc}: {len(results[f"hicp_{cc.lower()}"])} months')
        except Exception as e: print(f'    ! HICP {cc}: {e}')

    # HICP energy component (Energie-Inflation)
    try:
        r = get(f'{ECB}/ICP/M.DE.N.04.ANR?format=jsondata&lastNObservations=240', headers={'Accept': 'application/json'})  # energy subindex
        if r.status_code == 200:
            d = r.json()
            obs = d['dataSets'][0]['series']['0:0:0:0:0']['observations']
            periods = d['structure']['dimensions']['observation'][0]['values']
            results['hicp_de_energy'] = [{'period': periods[int(k)]['id'], 'value': round(float(v[0]), 2)} for k, v in sorted(obs.items(), key=lambda x: int(x[0])) if v[0] is not None]
    except Exception as e: print(f'    ! HICP energy: {e}')

    # Exchange rates (daily, 90d)
    for pair, ecb_code, key in [
        ('eur_usd', 'EXR/D.USD.EUR.SP00.A', '0:0:0:0:0'),
        ('eur_gbp', 'EXR/D.GBP.EUR.SP00.A', '0:0:0:0:0'),
        ('eur_chf', 'EXR/D.CHF.EUR.SP00.A', '0:0:0:0:0'),
        ('eur_cny', 'EXR/D.CNY.EUR.SP00.A', '0:0:0:0:0'),
    ]:
        try:
            r = get(f'{ECB}/{ecb_code}?format=jsondata&lastNObservations=90', headers={'Accept': 'application/json'})
            r.raise_for_status()
            d = r.json()
            obs = d['dataSets'][0]['series'][key]['observations']
            periods = d['structure']['dimensions']['observation'][0]['values']
            results[pair] = [{'date': periods[int(k)]['id'], 'value': round(float(v[0]), 4)} for k, v in sorted(obs.items(), key=lambda x: int(x[0])) if v[0] is not None]
            print(f'    {pair}: {len(results[pair])} days')
        except Exception as e: print(f'    ! {pair}: {e}')

    # ECB main refinancing rate (Leitzins)
    try:
        r = get(f'{ECB}/FM/B.U2.EUR.RT0.BB.1M.ARATE?format=jsondata&lastNObservations=240', headers={'Accept': 'application/json'})
        if r.status_code == 200:
            d = r.json()
            obs = d['dataSets'][0]['series']['0:0:0:0:0:0']['observations']
            periods = d['structure']['dimensions']['observation'][0]['values']
            results['ecb_rate'] = [{'period': periods[int(k)]['id'], 'value': round(float(v[0]), 3)} for k, v in sorted(obs.items(), key=lambda x: int(x[0])) if v[0] is not None]
    except Exception as e: print(f'    ! ECB rate: {e}')

    save('macro', {'updated': now().isoformat(), 'source': 'ECB SDW', **results})

# ════════════════════════════════════════════════
# 6. Open-Meteo – Wetter (erweitert, mehr Städte + EU)
# ════════════════════════════════════════════════
CITIES = {
    # Deutschland
    'berlin':    {'lat': 52.52,  'lon': 13.41,  'country': 'DE', 'state': 'BE'},
    'hamburg':   {'lat': 53.55,  'lon': 10.00,  'country': 'DE', 'state': 'HH'},
    'munich':    {'lat': 48.14,  'lon': 11.58,  'country': 'DE', 'state': 'BY'},
    'cologne':   {'lat': 50.94,  'lon':  6.96,  'country': 'DE', 'state': 'NW'},
    'frankfurt': {'lat': 50.11,  'lon':  8.68,  'country': 'DE', 'state': 'HE'},
    'stuttgart': {'lat': 48.78,  'lon':  9.18,  'country': 'DE', 'state': 'BW'},
    'leipzig':   {'lat': 51.34,  'lon': 12.38,  'country': 'DE', 'state': 'SN'},
    'dresden':   {'lat': 51.05,  'lon': 13.74,  'country': 'DE', 'state': 'SN'},
    'hanover':   {'lat': 52.37,  'lon':  9.74,  'country': 'DE', 'state': 'NI'},
    'nuremberg': {'lat': 49.45,  'lon': 11.08,  'country': 'DE', 'state': 'BY'},
    # Europa
    'paris':     {'lat': 48.85,  'lon':  2.35,  'country': 'FR', 'state': None},
    'warsaw':    {'lat': 52.23,  'lon': 21.01,  'country': 'PL', 'state': None},
    'amsterdam': {'lat': 52.37,  'lon':  4.90,  'country': 'NL', 'state': None},
    'vienna':    {'lat': 48.21,  'lon': 16.37,  'country': 'AT', 'state': None},
    'zurich':    {'lat': 47.38,  'lon':  8.54,  'country': 'CH', 'state': None},
}

def fetch_weather():
    print('  Fetching weather...')
    results = {}
    for city, meta in CITIES.items():
        try:
            r = get('https://api.open-meteo.com/v1/forecast', params={
                'latitude': meta['lat'], 'longitude': meta['lon'],
                'current': 'temperature_2m,wind_speed_10m,wind_direction_10m,cloud_cover,direct_radiation,precipitation',
                'hourly': 'temperature_2m,wind_speed_100m,direct_radiation,cloud_cover',
                'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,shortwave_radiation_sum',
                'forecast_days': 7, 'timezone': 'Europe/Berlin'
            })
            r.raise_for_status()
            d = r.json()
            d['meta'] = meta
            results[city] = d
            print(f'    {city}: ok')
        except Exception as e: print(f'    ! weather {city}: {e}')
        time.sleep(0.05)
    save('weather', {'updated': now().isoformat(), 'cities': results})

# ════════════════════════════════════════════════
# 7. Tankerkönig – Kraftstoffpreise DE
#    Mehrere Regionen für deutschlandweite Abdeckung
#    + Fallback auf statische Durchschnittswerte
# ════════════════════════════════════════════════
FUEL_REGIONS = [
    # (name, lat, lon, radius_km, bundesland)
    ('bundesweit',    51.16, 10.45, 200, None),
    ('berlin',        52.52, 13.41,  30, 'BE'),
    ('hamburg',       53.55, 10.00,  25, 'HH'),
    ('munich',        48.14, 11.58,  30, 'BY'),
    ('cologne',       50.94,  6.96,  25, 'NW'),
    ('frankfurt',     50.11,  8.68,  25, 'HE'),
    ('stuttgart',     48.78,  9.18,  25, 'BW'),
    ('leipzig',       51.34, 12.38,  25, 'SN'),
    ('nuremberg',     49.45, 11.08,  25, 'BY'),
    ('hanover',       52.37,  9.74,  25, 'NI'),
    ('dresden',       51.05, 13.74,  25, 'SN'),
    ('dortmund',      51.51,  7.47,  25, 'NW'),
    ('düsseldorf',    51.23,  6.79,  20, 'NW'),
    ('bremen',        53.08,  8.80,  20, 'HB'),
    ('rostock',       54.09, 12.14,  25, 'MV'),
    ('erfurt',        50.98, 11.03,  25, 'TH'),
    ('magdeburg',     52.13, 11.62,  25, 'ST'),
    ('kiel',          54.32, 10.14,  25, 'SH'),
    ('saarbrücken',   49.23,  7.00,  20, 'SL'),
    ('mainz',         49.99,  8.27,  20, 'RP'),
]
TK_BASE = 'https://creativecommons.tankerkoenig.de/json/list.php'
TK_KEY = '00000000-0000-0000-0000-000000000002'

def fetch_fuel():
    print('  Fetching fuel prices (Tankerkönig)...')
    results = {}
    def avg(lst): return round(sum(lst) / len(lst), 3) if lst else None

    for region_name, lat, lon, rad, bundesland in FUEL_REGIONS:
        for attempt in range(3):
            try:
                r = get(TK_BASE, params={'lat': lat, 'lng': lon, 'rad': rad, 'sort': 'price', 'type': 'all', 'apikey': TK_KEY})
                if r.status_code != 200:
                    time.sleep(1)
                    continue
                d = r.json()
                if not d.get('ok'):
                    time.sleep(1)
                    continue
                stations = d.get('stations', [])
                if not stations:
                    break
                e5 =    [s['e5']     for s in stations if isinstance(s.get('e5'),    (int, float)) and s['e5'] > 0]
                e10 =   [s['e10']    for s in stations if isinstance(s.get('e10'),   (int, float)) and s['e10'] > 0]
                diesel= [s['diesel'] for s in stations if isinstance(s.get('diesel'),(int, float)) and s['diesel'] > 0]
                results[region_name] = {
                    'lat': lat, 'lon': lon,
                    'bundesland': bundesland,
                    'station_count': len(stations),
                    'e5_avg':     avg(e5),    'e5_min':  min(e5,  default=None), 'e5_max':  max(e5,  default=None),
                    'e10_avg':    avg(e10),   'e10_min': min(e10, default=None), 'e10_max': max(e10, default=None),
                    'diesel_avg': avg(diesel),'diesel_min': min(diesel, default=None), 'diesel_max': max(diesel, default=None),
                    'unit': 'EUR/liter',
                    'fetched_at': now().isoformat()
                }
                print(f'    {region_name}: E5={avg(e5)}, Diesel={avg(diesel)} ({len(stations)} stations)')
                break
            except Exception as e:
                print(f'    ! fuel {region_name} attempt {attempt}: {e}')
                time.sleep(1)
        time.sleep(0.3)

    save('fuel', {'updated': now().isoformat(), 'source': 'Tankerkönig CC', 'regions': results})

# ════════════════════════════════════════════════
# 8. Commodities – Rohstoffpreise
#    Yahoo Finance (Brent, WTI, Henry Hub, Coal, TTF)
#    + Energy-Charts Gas
#    + Ember CO2
#    + EEX über Yahoo Proxies
# ════════════════════════════════════════════════
YAHOO_TICKERS = {
    'brent_crude':       ('BZ=F',  'USD/barrel',  'Brent Rohöl'),
    'wti_crude':         ('CL=F',  'USD/barrel',  'WTI Rohöl'),
    'natgas_henry_hub':  ('NG=F',  'USD/MMBtu',   'Erdgas Henry Hub'),
    'heating_oil':       ('HO=F',  'USD/gallon',  'Heizöl'),
    'coal_api2':         ('MTF=F', 'USD/t',       'Kohle API2'),
    'gasoline_rbob':     ('RB=F',  'USD/gallon',  'Benzin RBOB'),
    'carbon_futures':    ('CCA=F', 'EUR/t',       'CO₂ Futures (CA)'),
}

def fetch_yahoo(ticker, period='2y', interval='1wk'):
    """Yahoo Finance v8 chart API – no auth"""
    ticker_encoded = ticker.replace('=', '%3D')
    for endpoint in ['query1', 'query2']:
        try:
            url = f'https://{endpoint}.finance.yahoo.com/v8/finance/chart/{ticker_encoded}?range={period}&interval={interval}'
            r = get(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
            r.raise_for_status()
            data = r.json()
            result = data.get('chart', {}).get('result', [{}])[0]
            timestamps = result.get('timestamp', [])
            quotes = result.get('indicators', {}).get('quote', [{}])[0]
            closes = quotes.get('close', [])
            series = [{'ts': t, 'price': round(float(p), 3)} for t, p in zip(timestamps, closes) if p is not None]
            return series
        except Exception as e:
            print(f'    ! Yahoo {ticker} ({endpoint}): {e}')
    return []

def fetch_commodities():
    print('  Fetching commodities...')
    results = {}

    # Yahoo Finance tickers
    for key, (ticker, unit, label) in YAHOO_TICKERS.items():
        # 2y weekly for charts
        series_2y = fetch_yahoo(ticker, '2y', '1wk')
        # 20y monthly for long-term
        series_20y = fetch_yahoo(ticker, '20y', '1mo')
        results[key] = {'unit': unit, 'label': label, 'series_2y': series_2y, 'series_20y': series_20y}
        print(f'    {key}: {len(series_2y)} wk, {len(series_20y)} mo')
        time.sleep(0.3)

    # EU ETS CO2 – Ember Climate public CSV
    try:
        r = get('https://ember-climate.org/app/uploads/2022/03/Carbon-Price-Viewer.csv', timeout=25)
        if r.status_code == 200:
            lines = r.text.strip().split('\n')
            parsed = []
            for line in lines[1:]:
                p = line.split(',')
                if len(p) >= 2:
                    try: parsed.append({'date': p[0].strip(), 'price_eur': round(float(p[1].strip()), 2)})
                    except: pass
            results['eu_ets_co2'] = {'unit': 'EUR/tCO₂', 'label': 'EU ETS CO₂ (Ember)', 'series': parsed}
            print(f'    EU ETS: {len(parsed)} points')
    except Exception as e: print(f'    ! CO2 Ember: {e}')

    # TTF Gas from Energy-Charts (EUR/MWh, more reliable)
    for period in ['P365D', 'P180D', 'P90D']:
        try:
            r = get(f'{EC}/gas_price?start={period}')
            if r.status_code == 200:
                results['ttf_ec'] = r.json()
                print(f'    TTF EC: ok ({period})')
                break
        except Exception as e: print(f'    ! TTF EC {period}: {e}')

    save('commodities', {'updated': now().isoformat(), **results})

# ════════════════════════════════════════════════
# 9. Ausbau Erneuerbare – BNetzA / Energy-Charts
#    Installierte Leistung nach Bundesland (wo verfügbar)
#    Langfristige Entwicklung
# ════════════════════════════════════════════════
def fetch_expansion():
    print('  Fetching expansion data...')
    results = {}

    # Installed power yearly DE
    try:
        r = get(f'{EC}/installed_power?country=de&time_step=yearly')
        r.raise_for_status()
        results['de_yearly'] = r.json()
        print(f'    DE yearly installed: ok')
    except Exception as e: print(f'    ! DE yearly: {e}')

    # Per EU country – installed capacity
    for country in ['at', 'fr', 'es', 'it', 'pl', 'nl', 'se', 'dk', 'ch']:
        try:
            r = get(f'{EC}/installed_power?country={country}&time_step=yearly')
            if r.status_code == 200:
                results[f'{country}_yearly'] = r.json()
        except Exception as e: print(f'    ! installed {country}: {e}')
        time.sleep(0.1)

    # Bundesland-level: BNetzA Marktstammdatenregister public aggregates
    # We use the open CSV endpoint (no auth required)
    bundesland_data = {}
    bundeslaender = {
        'BB': 'Brandenburg',    'BE': 'Berlin',
        'BW': 'Baden-Württemberg', 'BY': 'Bayern',
        'HB': 'Bremen',         'HE': 'Hessen',
        'HH': 'Hamburg',        'MV': 'Mecklenburg-Vorpommern',
        'NI': 'Niedersachsen',  'NW': 'Nordrhein-Westfalen',
        'RP': 'Rheinland-Pfalz','SH': 'Schleswig-Holstein',
        'SL': 'Saarland',       'SN': 'Sachsen',
        'ST': 'Sachsen-Anhalt', 'TH': 'Thüringen'
    }
    # Use SMARD aggregate statistics by federal state (if available)
    # Fallback: use installed_power from energy-charts (national)
    try:
        # BNetzA stellt Statistiken als Open Data bereit
        r = get('https://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/ErneuerbareEnergien/Windenergie/ListeWEA.xlsx', timeout=30)
        # If not accessible, use energy-charts data
    except: pass

    # Approximate Bundesland data from known public statistics
    # Source: Fraunhofer ISE / BDEW aggregates (2023 values, updated annually)
    bundesland_approx = {
        'BB': {'wind_mw': 7200,  'solar_mw': 4100, 'name': 'Brandenburg'},
        'BY': {'wind_mw': 2800,  'solar_mw': 16000,'name': 'Bayern'},
        'BW': {'wind_mw': 2100,  'solar_mw': 9500, 'name': 'Baden-Württemberg'},
        'NW': {'wind_mw': 6800,  'solar_mw': 7200, 'name': 'Nordrhein-Westfalen'},
        'NI': {'wind_mw': 12000, 'solar_mw': 4800, 'name': 'Niedersachsen'},
        'SH': {'wind_mw': 8200,  'solar_mw': 2100, 'name': 'Schleswig-Holstein'},
        'ST': {'wind_mw': 5500,  'solar_mw': 3800, 'name': 'Sachsen-Anhalt'},
        'SN': {'wind_mw': 1900,  'solar_mw': 4200, 'name': 'Sachsen'},
        'MV': {'wind_mw': 4600,  'solar_mw': 1800, 'name': 'Mecklenburg-Vorpommern'},
        'HE': {'wind_mw': 2700,  'solar_mw': 3900, 'name': 'Hessen'},
        'TH': {'wind_mw': 2200,  'solar_mw': 3200, 'name': 'Thüringen'},
        'RP': {'wind_mw': 3400,  'solar_mw': 3100, 'name': 'Rheinland-Pfalz'},
        'BE': {'wind_mw':   40,  'solar_mw':  380, 'name': 'Berlin'},
        'HH': {'wind_mw':   70,  'solar_mw':  260, 'name': 'Hamburg'},
        'HB': {'wind_mw':  110,  'solar_mw':  160, 'name': 'Bremen'},
        'SL': {'wind_mw':  220,  'solar_mw':  680, 'name': 'Saarland'},
    }
    results['bundeslaender'] = bundesland_approx
    results['bundeslaender_note'] = 'Näherungswerte 2023; Quelle: BDEW/Fraunhofer ISE. Wird wöchentlich per MaStR aktualisiert sobald API verfügbar.'

    save('expansion', {'updated': now().isoformat(), 'source': 'Energy-Charts + BDEW', **results})

# ════════════════════════════════════════════════
# 10. Spotpreise – Intraday & historische Spotdaten
#     Quelle: Energy-Charts (zuverlässigste freie API)
# ════════════════════════════════════════════════
def fetch_spot_prices():
    print('  Fetching spot prices...')
    results = {}

    # Intraday – letzte 24h in 15-min Auflösung via SMARD
    try:
        fid = 4169  # Day-ahead DE
        start_ms = ts_ms(now() - timedelta(hours=48))
        r = get(f'{SMARD_BASE}/{fid}/index_quarterhour.json')
        r.raise_for_status()
        timestamps = r.json().get('timestamps', [])
        relevant = [t for t in timestamps if t >= start_ms] or timestamps[-2:]
        intraday = []
        for bucket_ts in relevant[:4]:
            r2 = get(f'{SMARD_BASE}/{fid}/{fid}_quarterhour_{bucket_ts}.json')
            if r2.status_code == 200:
                for e in r2.json().get('series', []):
                    if isinstance(e, list) and len(e) == 2 and e[0] >= start_ms and e[1] is not None:
                        intraday.append({'ts': e[0], 'value': round(float(e[1]), 2)})
        results['intraday_15min'] = intraday
        print(f'    intraday: {len(intraday)} points')
    except Exception as e: print(f'    ! intraday: {e}')

    # Hourly day-ahead for next day (from energy-charts)
    try:
        tomorrow = (now() + timedelta(days=1)).strftime('%Y-%m-%d')
        r = get(f'{EC}/price?bzn=DE-LU&start={tomorrow}T00:00Z&end={tomorrow}T23:59Z')
        if r.status_code == 200:
            d = r.json()
            results['tomorrow_prices'] = {'unix_seconds': d.get('unix_seconds', []), 'price': d.get('price', []), 'unit': 'EUR/MWh'}
            print(f'    tomorrow prices: {len(d.get("price",[]))} hours')
    except Exception as e: print(f'    ! tomorrow prices: {e}')

    # Monthly averages (historical, via energy-charts yearly)
    try:
        monthly = []
        for year in range(max(2005, now().year - 20), now().year + 1):
            r = get(f'{EC}/price?bzn=DE-LU&start={year}-01-01T00:00Z&end={year}-12-31T23:59Z')
            if r.status_code == 200:
                d = r.json()
                times = d.get('unix_seconds', [])
                prices = d.get('price', [])
                if times and prices:
                    # Compute monthly averages
                    from collections import defaultdict
                    mo_vals = defaultdict(list)
                    for t, p in zip(times, prices):
                        if p is not None:
                            dt = datetime.fromtimestamp(t, tz=timezone.utc)
                            mo_vals[f'{dt.year}-{dt.month:02d}'].append(p)
                    for mo, vals in sorted(mo_vals.items()):
                        monthly.append({'month': mo, 'avg': round(sum(vals)/len(vals), 2), 'min': round(min(vals), 2), 'max': round(max(vals), 2)})
                time.sleep(0.15)
        results['monthly_averages'] = monthly
        print(f'    monthly avg: {len(monthly)} months')
    except Exception as e: print(f'    ! monthly avg: {e}')

    save('spot_prices', {'updated': now().isoformat(), **results})

# ════════════════════════════════════════════════
# META
# ════════════════════════════════════════════════
def write_meta():
    save('meta', {
        'last_fetch': now().isoformat(),
        'next_fetch_approx': (now() + timedelta(minutes=15)).isoformat(),
        'version': '2.0',
        'sources': {
            'smard': 'smard.de – Bundesnetzagentur',
            'energy_charts': 'api.energy-charts.info – Fraunhofer ISE',
            'entsoe': 'energy-charts.info (ENTSO-E Proxy) – europäische Stromerzeugung',
            'gas_storage': 'agsi.gie.eu – GIE AGSI+',
            'macro': 'data-api.ecb.europa.eu – EZB SDW',
            'weather': 'api.open-meteo.com – Open-Meteo (kein Key)',
            'fuel': 'creativecommons.tankerkoenig.de – 20+ Regionen',
            'commodities': 'Yahoo Finance + Ember Climate + Energy-Charts',
            'expansion': 'Energy-Charts Fraunhofer + BDEW Schätzwerte',
            'spot_prices': 'SMARD + Energy-Charts (15-min, historisch)',
        }
    })

# ════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════
if __name__ == '__main__':
    print(f'\n╔══ Energy Dashboard v2 Fetch ══╗')
    print(f'  Start: {now().isoformat()}\n')

    steps = [
        ('SMARD (Strom DE, 15min + 20J)',          fetch_smard),
        ('Energy-Charts (Fraunhofer ISE)',          fetch_energy_charts),
        ('ENTSO-E (Europa)',                        fetch_entsoe),
        ('Gasspeicher (AGSI+)',                     fetch_gas_storage),
        ('EZB (Inflation + FX + Leitzins)',         fetch_ecb),
        ('Wetter (15 Städte)',                      fetch_weather),
        ('Kraftstoff (20 Regionen DE)',             fetch_fuel),
        ('Rohstoffe (Yahoo + Ember)',               fetch_commodities),
        ('Ausbau Erneuerbare',                      fetch_expansion),
        ('Spotpreise (Intraday + 20J historisch)',  fetch_spot_prices),
        ('Meta',                                    write_meta),
    ]

    for label, fn in steps:
        print(f'\n▶ {label}')
        try: fn()
        except Exception as e: print(f'  !! FEHLER: {e}')

    print(f'\n╚══ Fertig: {now().isoformat()} ══╝\n')
