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
                    fill = entry['status'].get('full_is_percentage')

                date = (entry.get('gasDayStart') or entry.get('date') or entry.get('datetime', ''))[:10]
                injection  = float(entry.get('injection')  or entry.get('injectionCapacity')  or 0)
                withdrawal = float(entry.get('withdrawal') or entry.get('withdrawalCapacity') or 0)

                cleaned.append({
                    'date':       date,
                    'fill_pct':   float(fill) if fill is not None else None,
                    'injection':  injection,
                    'withdrawal': withdrawal,
                })
            results[code] = {'name': name, 'data': cleaned}
            time.sleep(0.2)
        except Exception as e:
            print(f'  ! AGSI {code}: {e}')
            results[code] = {'name': name, 'data': [], 'error': str(e)}

    try:
        r = SESSION.get('https://alsi.gie.eu/api', params={'type': 'eu', 'size': 30}, timeout=20)
        r.raise_for_status()
        results['lng_eu'] = r.json().get('data', [])
    except Exception as e:
        print(f'  ! ALSI LNG: {e}')

    save('gas_storage', {'updated': now_utc().isoformat(), 'source': 'GIE AGSI+ / ALSI', **results})

# ════════════════════════════════════════════════
# 4. ECB – Inflation + FX
# ════════════════════════════════════════════════
def fetch_ecb():
    BASE = 'https://data-api.ecb.europa.eu/service/data'
    results = {}

    hicp_countries = {
        'DE': 'hicp_de', 'FR': 'hicp_fr', 'AT': 'hicp_at',
        'ES': 'hicp_es', 'IT': 'hicp_it', 'U2': 'hicp_eu',
    }
    for cc, result_key in hicp_countries.items():
        try:
            url = f'{BASE}/ICP/M.{cc}.N.000000.4.ANR?format=jsondata&lastNObservations=60'
            r = SESSION.get(url, timeout=20, headers={'Accept': 'application/json'})
            r.raise_for_status()
            d = r.json()
            datasets   = d.get('dataSets', [{}])
            series_all = datasets[0].get('series', {}) if datasets else {}
            first      = next(iter(series_all.values()), {})
            obs        = first.get('observations', {})
            periods    = d.get('structure', {}).get('dimensions', {}).get('observation', [{}])[0].get('values', [])
            series = [
                {'period': periods[int(k)]['id'], 'value': v[0]}
                for k, v in sorted(obs.items(), key=lambda x: int(x[0]))
                if periods and int(k) < len(periods)
            ]
            results[result_key] = {'unit': '% annual rate', 'country': cc, 'series': series}
        except Exception as e:
            print(f'  ! ECB HICP {cc}: {e}')
            results[result_key] = {}

    fx_pairs = {
        'eur_usd': 'EXR/D.USD.EUR.SP00.A',
        'eur_gbp': 'EXR/D.GBP.EUR.SP00.A',
        'eur_chf': 'EXR/D.CHF.EUR.SP00.A',
        'eur_cny': 'EXR/D.CNY.EUR.SP00.A',
    }
    for name, path in fx_pairs.items():
        try:
            url = f'{BASE}/{path}?format=jsondata&lastNObservations=252'
            r = SESSION.get(url, timeout=20, headers={'Accept': 'application/json'})
            r.raise_for_status()
            d = r.json()
            datasets   = d.get('dataSets', [{}])
            series_all = datasets[0].get('series', {}) if datasets else {}
            first      = next(iter(series_all.values()), {})
            obs        = first.get('observations', {})
            periods    = d.get('structure', {}).get('dimensions', {}).get('observation', [{}])[0].get('values', [])
            series = [
                {'date': periods[int(k)]['id'], 'value': round(v[0], 4)}
                for k, v in sorted(obs.items(), key=lambda x: int(x[0]))
                if periods and int(k) < len(periods)
            ]
            results[name] = series
        except Exception as e:
            print(f'  ! ECB FX {name}: {e}')
            results[name] = []

    save('macro', {'updated': now_utc().isoformat(), 'source': 'ECB SDW', **results})

# ════════════════════════════════════════════════
# 5. Open-Meteo – Weather
# ════════════════════════════════════════════════
CITIES = {
    'berlin':      (52.52,  13.41, 'DE'),
    'hamburg':     (53.55,  10.00, 'DE'),
    'munich':      (48.14,  11.58, 'DE'),
    'cologne':     (50.94,   6.96, 'DE'),
    'frankfurt':   (50.11,   8.68, 'DE'),
    'stuttgart':   (48.78,   9.18, 'DE'),
    'dusseldorf':  (51.22,   6.77, 'DE'),
    'leipzig':     (51.34,  12.38, 'DE'),
    'dresden':     (51.05,  13.74, 'DE'),
    'nuremberg':   (49.45,  11.08, 'DE'),
    'vienna':      (48.21,  16.37, 'AT'),
    'paris':       (48.85,   2.35, 'FR'),
    'amsterdam':   (52.37,   4.89, 'NL'),
    'warsaw':      (52.23,  21.01, 'PL'),
    'zurich':      (47.38,   8.54, 'CH'),
}

def fetch_weather():
    results = {}
    for city, (lat, lon, country) in CITIES.items():
        try:
            r = SESSION.get('https://api.open-meteo.com/v1/forecast', params={
                'latitude': lat, 'longitude': lon,
                'current':  'temperature_2m,wind_speed_10m,wind_direction_10m,cloud_cover,direct_radiation,precipitation',
                'hourly':   'temperature_2m,wind_speed_100m,wind_direction_100m,direct_radiation,diffuse_radiation,precipitation_probability',
                'daily':    'temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,shortwave_radiation_sum',
                'forecast_days': 5,
                'timezone': 'Europe/Berlin'
            }, timeout=15)
            r.raise_for_status()
            d = r.json()
            d['country'] = country
            results[city] = d
            time.sleep(0.05)
        except Exception as e:
            print(f'  ! weather {city}: {e}')

    save('weather', {'updated': now_utc().isoformat(), 'cities': results})

# ════════════════════════════════════════════════
# 6. Tankerkoenig – Fuel prices per city
# ════════════════════════════════════════════════
FUEL_CITIES = {
    'berlin':     (52.520, 13.405),
    'hamburg':    (53.550, 10.000),
    'munich':     (48.137, 11.575),
    'cologne':    (50.938,  6.960),
    'frankfurt':  (50.110,  8.682),
    'stuttgart':  (48.775,  9.182),
    'dusseldorf': (51.227,  6.773),
    'leipzig':    (51.340, 12.375),
    'nuremberg':  (49.452, 11.077),
    'dortmund':   (51.514,  7.465),
    'bremen':     (53.079,  8.801),
    'hannover':   (52.375,  9.735),
    'dresden':    (51.050, 13.740),
    'bochum':     (51.481,  7.216),
    'wuppertal':  (51.257,  7.150),
}

def fetch_tankerkoenig():
    results = {}
    key = '00000000-0000-0000-0000-000000000002'

    def avg(lst):
        return round(sum(lst) / len(lst), 3) if lst else None

    for city, (lat, lon) in FUEL_CITIES.items():
        try:
            r = SESSION.get(
                'https://creativecommons.tankerkoenig.de/json/list.php',
                params={'lat': lat, 'lng': lon, 'rad': 10, 'sort': 'price', 'type': 'all', 'apikey': key},
                timeout=15
            )
            r.raise_for_status()
            d = r.json()
            if not d.get('ok'):
                results[city] = {'error': d.get('message', 'api error')}
                continue

            stations = d.get('stations', [])
            e5  = [s['e5']     for s in stations if s.get('e5')     and s['e5']     > 0.5]
            e10 = [s['e10']    for s in stations if s.get('e10')    and s['e10']    > 0.5]
            die = [s['diesel'] for s in stations if s.get('diesel') and s['diesel'] > 0.5]

            results[city] = {
                'e5_avg':      avg(e5),
                'e5_min':      min(e5)  if e5  else None,
                'e5_max':      max(e5)  if e5  else None,
                'e10_avg':     avg(e10),
                'e10_min':     min(e10) if e10 else None,
                'e10_max':     max(e10) if e10 else None,
                'diesel_avg':  avg(die),
                'diesel_min':  min(die) if die else None,
                'diesel_max':  max(die) if die else None,
                'station_count': len(stations),
            }
            time.sleep(0.2)
        except Exception as e:
            print(f'  ! tankerkoenig {city}: {e}')
            results[city] = {'error': str(e)}

    # National average (200km radius around Germany center)
    try:
        r = SESSION.get(
            'https://creativecommons.tankerkoenig.de/json/list.php',
            params={'lat': 51.163, 'lng': 10.447, 'rad': 200, 'sort': 'price', 'type': 'all', 'apikey': key},
            timeout=25
        )
        r.raise_for_status()
        stations = r.json().get('stations', [])
        e5  = [s['e5']     for s in stations if s.get('e5')     and s['e5']     > 0.5]
        e10 = [s['e10']    for s in stations if s.get('e10')    and s['e10']    > 0.5]
        die = [s['diesel'] for s in stations if s.get('diesel') and s['diesel'] > 0.5]
        results['_national'] = {
            'e5_avg': avg(e5), 'e10_avg': avg(e10), 'diesel_avg': avg(die),
            'station_count': len(stations), 'unit': 'EUR/liter'
        }
    except Exception as e:
        print(f'  ! tankerkoenig national: {e}')

    save('fuel', {'updated': now_utc().isoformat(), 'source': 'Tankerkönig CC', 'cities': results})

# ════════════════════════════════════════════════
# 7. Yahoo Finance – Commodities
# ════════════════════════════════════════════════
YAHOO_TICKERS = {
    'brent_crude':   'BZ=F',
    'wti_crude':     'CL=F',
    'natgas_henry':  'NG=F',
    'coal_futures':  'MTF=F',
    'gold':          'GC=F',
    'carbon_eu':     'EUETS.DE',
}

def fetch_yahoo(ticker, range_='2y', interval='1d'):
    encoded = ticker.replace('=', '%3D')
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{encoded}?range={range_}&interval={interval}'
    r = SESSION.get(url, timeout=20, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
    })
    r.raise_for_status()
    result = r.json()['chart']['result'][0]
    ts     = result['timestamp']
    closes = result['indicators']['quote'][0]['close']
    return [{'ts': t, 'price': round(p, 4)} for t, p in zip(ts, closes) if p is not None]

def fetch_commodities():
    results = {}

    for name, ticker in YAHOO_TICKERS.items():
        try:
            results[name] = {'source': 'Yahoo Finance', 'series': fetch_yahoo(ticker)}
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! Yahoo {name} ({ticker}): {e}')
            results[name] = {'source': 'Yahoo Finance', 'series': [], 'error': str(e)}

    # EU ETS from Ember (CSV, no auth needed)
    try:
        r = SESSION.get(
            'https://ember-climate.org/app/uploads/2022/03/Carbon-Price-Viewer.csv',
            timeout=20
        )
        if r.status_code == 200:
            lines = r.text.strip().split('\n')
            parsed = []
            for line in lines[1:]:
                p = line.split(',')
                if len(p) >= 2:
                    try:
                        parsed.append({'date': p[0].strip(), 'price_eur': float(p[1].strip())})
                    except:
                        pass
            results['eu_ets_ember'] = {'source': 'Ember Climate', 'unit': 'EUR/tCO2', 'series': parsed}
    except Exception as e:
        print(f'  ! Ember CO2: {e}')

    # TTF fallback from Energy-Charts
    try:
        r = SESSION.get('https://api.energy-charts.info/gas_price?start=P730D', timeout=20)
        if r.status_code == 200:
            results['ttf_energy_charts'] = {'source': 'Energy-Charts/Fraunhofer', **r.json()}
    except Exception as e:
        print(f'  ! EC TTF: {e}')

    save('commodities', {'updated': now_utc().isoformat(), **results})

# ════════════════════════════════════════════════
# 8. Bundesland capacity (OPSD)
# ════════════════════════════════════════════════
def fetch_bundesland_capacity():
    results = {}

    try:
        url = 'https://data.open-power-system-data.org/renewable_power_plants/latest/renewable_power_plants_DE.csv'
        r = SESSION.get(url, timeout=60, stream=True)
        content = b''
        for chunk in r.iter_content(chunk_size=65536):
            content += chunk
            if len(content) > 5 * 1024 * 1024:
                break

        lines = content.decode('utf-8', errors='replace').split('\n')
        header = [h.strip().strip('"') for h in lines[0].split(',')]

        def find_col(candidates):
            for c in candidates:
                if c in header:
                    return header.index(c)
            return -1

        state_idx    = find_col(['federal_state', 'Bundesland', 'state'])
        type_idx     = find_col(['energy_source_level_2', 'energy_source', 'type', 'Energietraeger'])
        capacity_idx = find_col(['electrical_capacity', 'capacity_net_bnetza', 'Nettonennleistung'])

        if state_idx < 0 or capacity_idx < 0:
            raise ValueError(f'Columns not found. Header: {header[:15]}')

        aggregated = {}
        for line in lines[1:]:
            parts = line.split(',')
            if len(parts) <= max(state_idx, capacity_idx):
                continue
            try:
                state = parts[state_idx].strip().strip('"')
                etype = parts[type_idx].strip().strip('"') if type_idx >= 0 else 'unknown'
                cap   = float(parts[capacity_idx].strip().replace('"', '') or 0)
                if not state:
                    continue
                if state not in aggregated:
                    aggregated[state] = {}
                aggregated[state][etype] = round(aggregated[state].get(etype, 0) + cap / 1000, 4)
            except (ValueError, IndexError):
                pass

        results['by_bundesland'] = aggregated
        results['source'] = 'Open Power System Data (OPSD)'
    except Exception as e:
        print(f'  ! OPSD: {e}')
        results['by_bundesland'] = {}
        results['error'] = str(e)

    # Fallback national from Energy-Charts
    try:
        r = SESSION.get('https://api.energy-charts.info/installed_power?country=de&time_step=yearly', timeout=20)
        r.raise_for_status()
        results['national_yearly'] = r.json()
    except Exception as e:
        print(f'  ! EC national: {e}')

    save('bundesland', {'updated': now_utc().isoformat(), **results})

# ════════════════════════════════════════════════
# 9. Spot price history (20yr monthly averages)
# ════════════════════════════════════════════════
def fetch_spot_history():
    BASE = 'https://api.energy-charts.info'
    results = {}

    for country_code, bzn in [('de','DE-LU'), ('fr','FR'), ('at','AT'), ('es','ES')]:
        try:
            r = SESSION.get(f'{BASE}/price', params={
                'bzn': bzn,
                'start': '2005-01-01',
                'end': now_utc().strftime('%Y-%m-%d'),
                'interval': 'month'
            }, timeout=30)
            r.raise_for_status()
            d = r.json()
            results[f'monthly_{country_code}'] = {
                'bzn': bzn,
                'unix_seconds': d.get('unix_seconds', []),
                'price': d.get('price', []),
                'unit': 'EUR/MWh'
            }
            time.sleep(0.2)
        except Exception as e:
            print(f'  ! spot history {country_code}: {e}')

    try:
        r = SESSION.get(f'{BASE}/price', params={
            'bzn': 'DE-LU',
            'start': '2000-01-01',
            'end': now_utc().strftime('%Y-%m-%d'),
            'interval': 'year'
        }, timeout=30)
        r.raise_for_status()
        d = r.json()
        results['yearly_de'] = {
            'unix_seconds': d.get('unix_seconds', []),
            'price': d.get('price', []),
            'unit': 'EUR/MWh'
        }
    except Exception as e:
        print(f'  ! yearly prices: {e}')

    save('spot_history', {
        'updated': now_utc().isoformat(),
        'source': 'Energy-Charts / Fraunhofer ISE',
        **results
    })

# ════════════════════════════════════════════════
# 10. Metadata
# ════════════════════════════════════════════════
def write_meta():
    save('meta', {
        'last_fetch': now_utc().isoformat(),
        'next_fetch_approx': (now_utc() + timedelta(minutes=15)).isoformat(),
        'version': '2.0',
        'sources': {
            'smard':         'smard.de – Bundesnetzagentur',
            'energy_charts': 'api.energy-charts.info – Fraunhofer ISE',
            'gas_storage':   'agsi.gie.eu – GIE AGSI+',
            'macro':         'data-api.ecb.europa.eu – EZB',
            'weather':       'api.open-meteo.com – Open-Meteo',
            'fuel':          'creativecommons.tankerkoenig.de',
            'commodities':   'Yahoo Finance + Ember Climate',
            'bundesland':    'open-power-system-data.org – OPSD',
            'spot_history':  'api.energy-charts.info – Fraunhofer ISE',
        }
    })

# ════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════
if __name__ == '__main__':
    print(f'=== Energy Dashboard Fetch v2 – {now_utc().isoformat()} ===\n')
    steps = [
        ('SMARD (15-min Erzeugung)',         fetch_smard),
        ('SMARD History (20yr)',             fetch_smard_history),
        ('Energy-Charts (Fraunhofer ISE)',   fetch_energy_charts),
        ('AGSI+ Gas Storage',               fetch_agsi),
        ('ECB (Inflation + FX)',            fetch_ecb),
        ('Open-Meteo (15 Städte)',          fetch_weather),
        ('Tankerkoenig (15 Städte)',        fetch_tankerkoenig),
        ('Commodities (Yahoo + Ember)',     fetch_commodities),
        ('Bundesland Kapazitäten (OPSD)',   fetch_bundesland_capacity),
        ('Spot History (20yr monatlich)',   fetch_spot_history),
        ('Metadata',                        write_meta),
    ]
    for label, fn in steps:
        print(f'[{label}]')
        try:
            fn()
        except Exception as e:
            print(f'  !! FEHLER: {e}')
        print()
    print(f'=== Fertig: {now_utc().isoformat()} ===')
