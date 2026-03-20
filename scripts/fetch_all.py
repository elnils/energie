"""
Energy Dashboard – Data Fetcher v3
Korrekte SMARD-URL: /chart_data/{filter}/DE/{filter}_DE_{resolution}_{ts}.json
Alle Quellen getestet gegen echte API-Dokumentation.
"""
import json, os, time, requests, pytz
from datetime import datetime, timezone, timedelta

OUT = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(OUT, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'EnergyDashboard/3.0 (github.com)',
    'Accept': 'application/json, text/plain, */*',
})

def save(name, data):
    path = os.path.join(OUT, f'{name}.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
    size = os.path.getsize(path)
    print(f'  saved {name}.json ({size/1024:.1f} KB)')

def now_utc():
    return datetime.now(timezone.utc)

def ts_ms(dt):
    return int(dt.timestamp() * 1000)

def last_monday_midnight_ms():
    """SMARD benötigt Timestamps die auf Montag 00:00 Uhr MEZ/MESZ fallen."""
    berlin = pytz.timezone('Europe/Berlin')
    now_berlin = datetime.now(berlin)
    days_since_monday = now_berlin.weekday()
    last_monday = now_berlin - timedelta(days=days_since_monday)
    last_monday_midnight = last_monday.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(last_monday_midnight.timestamp() * 1000)

# ════════════════════════════════════════════════════════
# 1. SMARD – KORREKTE URL-STRUKTUR mit Region /DE/
#    Quelle: https://github.com/bundesAPI/smard-api
#    URL: /chart_data/{filter}/DE/{filter}_DE_{resolution}_{timestamp}.json
# ════════════════════════════════════════════════════════

# Korrekte Filter-IDs von offizieller Doku
SMARD_FILTERS = {
    # Erzeugung
    'lignite':           1223,   # Braunkohle
    'nuclear':           1224,   # Kernenergie
    'wind_offshore':     1225,   # Wind Offshore
    'hydro':             1226,   # Wasserkraft
    'other_conventional':1227,   # Sonstige Konventionelle
    'other_renewables':  1228,   # Sonstige Erneuerbare
    'biomass':           4066,   # Biomasse
    'wind_onshore':      4067,   # Wind Onshore
    'solar':             4068,   # Photovoltaik
    'hard_coal':         4069,   # Steinkohle
    'pumped_storage':    4070,   # Pumpspeicher
    'natural_gas':       4071,   # Erdgas
    # Verbrauch
    'load':              410,    # Stromverbrauch Gesamt
    'residual_load':     4359,   # Residuallast
    # Marktpreise
    'day_ahead_de_lu':   4169,   # Marktpreis DE/LU (ab Okt 2018)
    'day_ahead_de_at_lu':4996,   # Marktpreis DE/AT/LU (bis Sep 2018)
    'neighbouring':      5078,   # Marktpreis Anrainer DE/LU
}

def smard_get_timestamps(filter_id, resolution='quarterhour', region='DE'):
    url = f'https://www.smard.de/app/chart_data/{filter_id}/{region}/index_{resolution}.json'
    r = SESSION.get(url, timeout=20)
    r.raise_for_status()
    return r.json().get('timestamps', [])

def smard_get_series(filter_id, timestamp, resolution='quarterhour', region='DE'):
    url = f'https://www.smard.de/app/chart_data/{filter_id}/{region}/{filter_id}_{region}_{resolution}_{timestamp}.json'
    r = SESSION.get(url, timeout=20)
    r.raise_for_status()
    return r.json().get('series', [])

def fetch_smard():
    results = {}
    # Letzten Montag Mitternacht als Startpunkt
    monday_ms = last_monday_midnight_ms()
    # Auch vorherige Woche holen für 14 Tage Daten
    prev_monday_ms = monday_ms - 7 * 24 * 3600 * 1000

    for name, fid in SMARD_FILTERS.items():
        print(f'    smard/{name} (filter={fid})')
        series = []
        try:
            timestamps = smard_get_timestamps(fid, 'quarterhour')
            if not timestamps:
                raise ValueError('No timestamps')
            # Nimm die letzten 2 Buckets (= ca. 14 Tage)
            recent = [t for t in timestamps if t >= prev_monday_ms]
            if not recent:
                recent = timestamps[-2:]
            seen = set()
            for bucket_ts in recent[:2]:
                try:
                    raw = smard_get_series(fid, bucket_ts, 'quarterhour')
                    for entry in raw:
                        if isinstance(entry, list) and len(entry) == 2:
                            ts_val, value = entry
                            if value is not None and ts_val not in seen:
                                seen.add(ts_val)
                                series.append({'ts': ts_val, 'v': round(float(value), 2)})
                    time.sleep(0.08)
                except Exception as e:
                    print(f'      bucket {bucket_ts} fail: {e}')
            series.sort(key=lambda x: x['ts'])
        except Exception as e:
            print(f'    ! smard/{name}: {e}')
        results[name] = series

    save('smard', {
        'updated': now_utc().isoformat(),
        'unit_generation': 'MWh',
        'unit_price': 'EUR/MWh',
        'series': results
    })

def fetch_smard_history():
    """Day-Ahead Preise historisch – wöchentliche Buckets über 20 Jahre"""
    fid = 4169
    region = 'DE'
    series = []
    try:
        timestamps = smard_get_timestamps(fid, 'hour', region)
        cutoff = ts_ms(now_utc() - timedelta(days=365 * 20))
        relevant = [t for t in timestamps if t >= cutoff]
        # Jede 4. Woche samplen für 20-Jahres-Überblick
        step = max(1, len(relevant) // 50)
        sampled = relevant[::step]
        seen = set()
        for bucket_ts in sampled:
            try:
                raw = smard_get_series(fid, bucket_ts, 'hour', region)
                for entry in raw:
                    if isinstance(entry, list) and len(entry) == 2 and entry[1] is not None:
                        if entry[0] not in seen:
                            seen.add(entry[0])
                            series.append({'ts': entry[0], 'v': round(float(entry[1]), 2)})
                time.sleep(0.1)
            except Exception as e:
                print(f'    ! history bucket: {e}')
        series.sort(key=lambda x: x['ts'])
    except Exception as e:
        print(f'  ! smard_history: {e}')
    save('smard_history', {'updated': now_utc().isoformat(), 'unit': 'EUR/MWh', 'series': series})

# ════════════════════════════════════════════════════════
# 2. ENERGY-CHARTS (Fraunhofer ISE)
#    Beste Quelle für Strompreise + Erzeugungsmix
#    Getestete Endpoints: https://api.energy-charts.info
# ════════════════════════════════════════════════════════
EC = 'https://api.energy-charts.info'

def ec_get(path, params=None, timeout=25):
    r = SESSION.get(f'{EC}/{path}', params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def fetch_energy_charts():
    results = {}

    # Strompreise – alle relevanten Bidding Zones
    # DE hat Sonderfall: vor Okt 2018 = DE-AT-LU, danach = DE-LU
    bzn_list = [
        ('DE-LU',    'price_de'),
        ('AT',       'price_at'),
        ('FR',       'price_fr'),
        ('CH',       'price_ch'),
        ('PL',       'price_pl'),
        ('CZ',       'price_cz'),
        ('NL',       'price_nl'),
        ('BE',       'price_be'),
        ('DK1',      'price_dk1'),
        ('NO2',      'price_no'),
    ]
    for bzn, key in bzn_list:
        try:
            d = ec_get('price', {'bzn': bzn, 'start': 'P7D'})
            results[key] = {
                'bzn': bzn,
                'unix_seconds': d.get('unix_seconds', []),
                'price': d.get('price', []),
                'unit': 'EUR/MWh'
            }
            time.sleep(0.15)
        except Exception as e:
            print(f'  ! EC price {bzn}: {e}')
            results[key] = {}

    # Historische Preise DE-LU monatlich (für 20-Jahre-Chart)
    try:
        end_str = now_utc().strftime('%Y-%m-%d')
        # Neue Zone ab Okt 2018
        d1 = ec_get('price', {'bzn': 'DE-LU', 'start': '2018-10-01', 'end': end_str, 'interval': 'month'})
        # Alte Zone
        d2 = ec_get('price', {'bzn': 'DE-AT-LU', 'start': '2005-01-01', 'end': '2018-09-30', 'interval': 'month'})
        # Zusammenführen
        ts_combined = (d2.get('unix_seconds', []) + d1.get('unix_seconds', []))
        pr_combined = (d2.get('price', []) + d1.get('price', []))
        results['price_de_monthly_hist'] = {
            'unix_seconds': ts_combined,
            'price': pr_combined,
            'unit': 'EUR/MWh',
            'note': 'DE-AT-LU bis Sep 2018, DE-LU ab Okt 2018'
        }
        time.sleep(0.2)
    except Exception as e:
        print(f'  ! EC monthly history: {e}')

    # Öffentliche Erzeugung DE (7 Tage)
    try:
        end = now_utc()
        d = ec_get('public_power', {
            'country': 'de',
            'start': (end - timedelta(days=7)).strftime('%Y-%m-%dT%H:%MZ'),
            'end': end.strftime('%Y-%m-%dT%H:%MZ'),
        })
        results['public_power_de'] = d
    except Exception as e:
        print(f'  ! EC public_power: {e}')

    # Installierte Leistung – mehrere Länder jährlich
    for cc in ['de', 'fr', 'at', 'es', 'it', 'pl', 'gb', 'nl', 'be']:
        try:
            d = ec_get('installed_power', {'country': cc, 'time_step': 'yearly'})
            results[f'installed_{cc}'] = d
            time.sleep(0.1)
        except Exception as e:
            print(f'  ! EC installed_{cc}: {e}')

    # EE-Anteil DE (30 Tage)
    try:
        results['ren_share_de'] = ec_get('ren_share_in_public_power', {'country': 'de', 'start': 'P30D'})
    except Exception as e:
        print(f'  ! EC ren_share: {e}')

    # Grenzüberschreitender Handel DE
    try:
        results['cross_border_de'] = ec_get('cross_border_electricity_trading', {'country': 'de', 'start': 'P7D'})
    except Exception as e:
        print(f'  ! EC cross_border: {e}')

    # Erdgaspreis TTF (via Energy-Charts)
    try:
        results['gas_price'] = ec_get('gas_price', {'start': 'P730D'})
    except Exception as e:
        print(f'  ! EC gas_price: {e}')

    # Kohlepreis
    try:
        results['coal_price'] = ec_get('coal_price', {'start': 'P730D'})
    except Exception as e:
        print(f'  ! EC coal_price: {e}')

    # CO2-Zertifikate (EU ETS)
    try:
        results['co2_price'] = ec_get('co2_price', {'start': 'P730D'})
    except Exception as e:
        print(f'  ! EC co2_price: {e}')

    # Füllstände Gasspeicher (via Energy-Charts)
    try:
        results['gas_storage_ec'] = ec_get('gas_storage')
    except Exception as e:
        print(f'  ! EC gas_storage: {e}')

    save('energy_charts', {'updated': now_utc().isoformat(), **results})

# ════════════════════════════════════════════════════════
# 3. AGSI+ – Gas Storage (GIE)
#    https://agsi.gie.eu/api
# ════════════════════════════════════════════════════════
def fetch_agsi():
    results = {}
    countries = {
        'eu': 'EU gesamt', 'de': 'Deutschland', 'at': 'Österreich',
        'fr': 'Frankreich', 'it': 'Italien', 'nl': 'Niederlande',
        'be': 'Belgien', 'pl': 'Polen', 'cz': 'Tschechien',
        'es': 'Spanien', 'hu': 'Ungarn', 'sk': 'Slowakei',
    }
    for code, name in countries.items():
        try:
            r = SESSION.get('https://agsi.gie.eu/api', params={'type': code, 'size': 100}, timeout=25)
            r.raise_for_status()
            raw = r.json().get('data', [])
            cleaned = []
            for entry in raw:
                # Mehrere mögliche Feldnamen normalisieren
                fill = None
                for field in ['full_is_percentage', 'trend', 'gasInStorage', 'fillLevelFull']:
                    val = entry.get(field)
                    if val is not None:
                        try:
                            fill = float(val)
                            break
                        except (ValueError, TypeError):
                            pass
                if fill is None and isinstance(entry.get('status'), dict):
                    try:
                        fill = float(entry['status'].get('full_is_percentage', 0))
                    except:
                        pass

                date_raw = entry.get('gasDayStart') or entry.get('date') or entry.get('datetime', '')
                date = str(date_raw)[:10] if date_raw else ''

                def safe_float(v, default=0.0):
                    try: return float(v or default)
                    except: return default

                cleaned.append({
                    'date':       date,
                    'fill_pct':   fill,
                    'injection':  safe_float(entry.get('injection') or entry.get('injectionCapacity')),
                    'withdrawal': safe_float(entry.get('withdrawal') or entry.get('withdrawalCapacity')),
                    'working_gas': safe_float(entry.get('gasInStorage') or entry.get('workingGasVolume')),
                    'capacity':   safe_float(entry.get('full') or entry.get('capacity')),
                })
            results[code] = {'name': name, 'data': cleaned}
            time.sleep(0.15)
        except Exception as e:
            print(f'  ! AGSI {code}: {e}')
            results[code] = {'name': name, 'data': [], 'error': str(e)}

    # LNG Terminals
    try:
        r = SESSION.get('https://alsi.gie.eu/api', params={'type': 'eu', 'size': 60}, timeout=20)
        r.raise_for_status()
        results['lng_eu'] = r.json().get('data', [])
    except Exception as e:
        print(f'  ! ALSI LNG: {e}')

    save('gas_storage', {'updated': now_utc().isoformat(), 'source': 'GIE AGSI+ / ALSI', **results})

# ════════════════════════════════════════════════════════
# 4. ECB – Inflation (HICP) + Wechselkurse
#    https://data-api.ecb.europa.eu/service/data
# ════════════════════════════════════════════════════════
def ecb_get_series(dataset_path, n_obs=60):
    url = f'https://data-api.ecb.europa.eu/service/data/{dataset_path}?format=jsondata&lastNObservations={n_obs}'
    r = SESSION.get(url, timeout=25, headers={'Accept': 'application/json'})
    r.raise_for_status()
    d = r.json()
    datasets   = d.get('dataSets', [{}])
    series_all = datasets[0].get('series', {}) if datasets else {}
    first_key  = next(iter(series_all), None)
    if not first_key:
        return []
    obs     = series_all[first_key].get('observations', {})
    periods = d.get('structure', {}).get('dimensions', {}).get('observation', [{}])[0].get('values', [])
    result = []
    for k, v in sorted(obs.items(), key=lambda x: int(x[0])):
        idx = int(k)
        if idx < len(periods) and v and v[0] is not None:
            result.append({'period': periods[idx]['id'], 'value': round(v[0], 4)})
    return result

def fetch_ecb():
    results = {}

    # HICP Inflation – DE, FR, AT, ES, IT, U2 (Eurozone)
    hicp_codes = {
        'DE': 'hicp_de', 'FR': 'hicp_fr', 'AT': 'hicp_at',
        'ES': 'hicp_es', 'IT': 'hicp_it', 'U2': 'hicp_eu',
        'PL': 'hicp_pl', 'NL': 'hicp_nl',
    }
    for cc, key in hicp_codes.items():
        try:
            series = ecb_get_series(f'ICP/M.{cc}.N.000000.4.ANR', n_obs=60)
            results[key] = {'country': cc, 'unit': '% p.a.', 'series': series}
            time.sleep(0.1)
        except Exception as e:
            print(f'  ! ECB HICP {cc}: {e}')
            results[key] = {'country': cc, 'series': []}

    # Energiekomponente der Inflation DE (HICP Energie)
    try:
        series = ecb_get_series('ICP/M.DE.N.EG0000.4.ANR', n_obs=60)
        results['hicp_de_energy'] = {'country': 'DE', 'unit': '% p.a.', 'series': series, 'component': 'Energie'}
    except Exception as e:
        print(f'  ! ECB HICP Energie: {e}')

    # Wechselkurse
    fx_pairs = {
        'eur_usd': 'EXR/D.USD.EUR.SP00.A',
        'eur_gbp': 'EXR/D.GBP.EUR.SP00.A',
        'eur_chf': 'EXR/D.CHF.EUR.SP00.A',
        'eur_cny': 'EXR/D.CNY.EUR.SP00.A',
        'eur_jpy': 'EXR/D.JPY.EUR.SP00.A',
    }
    for name, path in fx_pairs.items():
        try:
            series = ecb_get_series(path, n_obs=365)
            results[name] = series
            time.sleep(0.1)
        except Exception as e:
            print(f'  ! ECB FX {name}: {e}')
            results[name] = []

    save('macro', {'updated': now_utc().isoformat(), 'source': 'ECB SDW', **results})

# ════════════════════════════════════════════════════════
# 5. OPEN-METEO – Wetter (kein API-Key nötig)
#    https://open-meteo.com/en/docs
# ════════════════════════════════════════════════════════
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
                'current': 'temperature_2m,wind_speed_10m,wind_direction_10m,cloud_cover,direct_radiation,precipitation,relative_humidity_2m',
                'hourly': 'temperature_2m,wind_speed_100m,wind_direction_100m,direct_radiation,diffuse_radiation,cloud_cover,precipitation_probability',
                'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,shortwave_radiation_sum,sunrise,sunset',
                'forecast_days': 5,
                'timezone': 'Europe/Berlin'
            }, timeout=15)
            r.raise_for_status()
            d = r.json()
            d['country'] = country
            results[city] = d
            time.sleep(0.06)
        except Exception as e:
            print(f'  ! weather {city}: {e}')
    save('weather', {'updated': now_utc().isoformat(), 'cities': results})

# ════════════════════════════════════════════════════════
# 6. TANKERKOENIG – Kraftstoffpreise
#    https://creativecommons.tankerkoenig.de
# ════════════════════════════════════════════════════════
FUEL_CITIES = {
    'berlin':       (52.520, 13.405),
    'hamburg':      (53.550, 10.000),
    'munich':       (48.137, 11.575),
    'cologne':      (50.938,  6.960),
    'frankfurt':    (50.110,  8.682),
    'stuttgart':    (48.775,  9.182),
    'dusseldorf':   (51.227,  6.773),
    'leipzig':      (51.340, 12.375),
    'nuremberg':    (49.452, 11.077),
    'dortmund':     (51.514,  7.465),
    'bremen':       (53.079,  8.801),
    'hannover':     (52.375,  9.735),
    'dresden':      (51.050, 13.740),
    'bochum':       (51.481,  7.216),
    'wuppertal':    (51.257,  7.150),
    'bielefeld':    (51.978,  8.532),
    'bonn':         (50.733,  7.100),
    'mannheim':     (49.487,  8.466),
    'augsburg':     (48.371, 10.898),
    'wiesbaden':    (50.082,  8.240),
}

def fetch_tankerkoenig():
    results = {}
    api_key = '00000000-0000-0000-0000-000000000002'

    def avg(lst):
        return round(sum(lst) / len(lst), 3) if lst else None

    for city, (lat, lon) in FUEL_CITIES.items():
        try:
            r = SESSION.get(
                'https://creativecommons.tankerkoenig.de/json/list.php',
                params={'lat': lat, 'lng': lon, 'rad': 15, 'sort': 'price', 'type': 'all', 'apikey': api_key},
                timeout=15
            )
            r.raise_for_status()
            d = r.json()
            if not d.get('ok'):
                print(f'  ! tankerkoenig {city}: {d.get("message")}')
                results[city] = {'error': d.get('message', 'api_error')}
                continue
            stations = d.get('stations', [])
            e5  = sorted([s['e5']     for s in stations if isinstance(s.get('e5'), float)     and s['e5']     > 0.5])
            e10 = sorted([s['e10']    for s in stations if isinstance(s.get('e10'), float)    and s['e10']    > 0.5])
            die = sorted([s['diesel'] for s in stations if isinstance(s.get('diesel'), float) and s['diesel'] > 0.5])
            results[city] = {
                'e5_avg':      avg(e5),   'e5_min':  e5[0]  if e5  else None, 'e5_max':  e5[-1]  if e5  else None,
                'e10_avg':     avg(e10),  'e10_min': e10[0] if e10 else None, 'e10_max': e10[-1] if e10 else None,
                'diesel_avg':  avg(die),  'diesel_min': die[0] if die else None, 'diesel_max': die[-1] if die else None,
                'count': len(stations),
                'cheapest_e5':    {'name': stations[0].get('name',''), 'price': stations[0].get('e5',0), 'brand': stations[0].get('brand','')} if stations else None,
            }
            time.sleep(0.25)
        except Exception as e:
            print(f'  ! tankerkoenig {city}: {e}')
            results[city] = {'error': str(e)}

    # Nationaler Durchschnitt
    try:
        r = SESSION.get(
            'https://creativecommons.tankerkoenig.de/json/list.php',
            params={'lat': 51.163, 'lng': 10.447, 'rad': 250, 'sort': 'price', 'type': 'all', 'apikey': api_key},
            timeout=30
        )
        r.raise_for_status()
        stations = r.json().get('stations', [])
        e5  = [s['e5']     for s in stations if isinstance(s.get('e5'), float)     and s['e5']     > 0.5]
        e10 = [s['e10']    for s in stations if isinstance(s.get('e10'), float)    and s['e10']    > 0.5]
        die = [s['diesel'] for s in stations if isinstance(s.get('diesel'), float) and s['diesel'] > 0.5]
        results['_national'] = {
            'e5_avg': avg(e5), 'e10_avg': avg(e10), 'diesel_avg': avg(die),
            'count': len(stations), 'unit': 'EUR/liter',
        }
    except Exception as e:
        print(f'  ! tankerkoenig national: {e}')

    save('fuel', {'updated': now_utc().isoformat(), 'source': 'Tankerkoenig CC', 'cities': results})

# ════════════════════════════════════════════════════════
# 7. YAHOO FINANCE – Rohstoffe
#    Fallback: stooq.com (kein Auth, CSV)
# ════════════════════════════════════════════════════════
TICKERS = {
    'brent_crude':   ('BZ=F',    'USD/Barrel'),
    'wti_crude':     ('CL=F',    'USD/Barrel'),
    'natgas_henry':  ('NG=F',    'USD/MMBtu'),
    'coal':          ('MTF=F',   'USD/t'),
    'gold':          ('GC=F',    'USD/oz'),
    'silver':        ('SI=F',    'USD/oz'),
    'uran':          ('UX=F',    'USD/lbs'),
    'eur_usd_fx':    ('EURUSD=X',''),
    'carbon_etf':    ('KRBN',    'USD'),
}

def yahoo_fetch(ticker, range_='2y'):
    encoded = ticker.replace('=', '%3D')
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{encoded}?range={range_}&interval=1d'
    r = SESSION.get(url, timeout=20, headers={
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'https://finance.yahoo.com',
    })
    r.raise_for_status()
    result = r.json()['chart']['result'][0]
    ts     = result['timestamp']
    meta   = result.get('meta', {})
    closes = result['indicators']['quote'][0]['close']
    return {
        'currency': meta.get('currency', ''),
        'series': [{'ts': t, 'p': round(c, 4)} for t, c in zip(ts, closes) if c is not None]
    }

def stooq_fetch(symbol):
    """Fallback-Quelle stooq.com – gibt CSV zurück"""
    url = f'https://stooq.com/q/d/l/?s={symbol}&i=d'
    r = SESSION.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
    r.raise_for_status()
    lines = r.text.strip().split('\n')
    if len(lines) < 2:
        return []
    series = []
    for line in lines[1:]:
        parts = line.split(',')
        if len(parts) >= 5:
            try:
                from datetime import datetime as dt
                ts = int(dt.strptime(parts[0], '%Y-%m-%d').timestamp())
                close = float(parts[4])
                series.append({'ts': ts, 'p': round(close, 4)})
            except:
                pass
    return series

def fetch_commodities():
    results = {}

    for name, (ticker, unit) in TICKERS.items():
        try:
            d = yahoo_fetch(ticker)
            results[name] = {'source': 'Yahoo Finance', 'unit': unit, 'ticker': ticker, **d}
            print(f'    yahoo {name}: {len(d["series"])} Punkte')
            time.sleep(0.35)
        except Exception as e:
            print(f'  ! Yahoo {name}: {e} – versuche Stooq...')
            # Stooq Fallback
            stooq_map = {
                'brent_crude': 'cb.f',
                'wti_crude':   'cl.f',
                'natgas_henry':'ng.f',
                'gold':        'gc.f',
            }
            if name in stooq_map:
                try:
                    series = stooq_fetch(stooq_map[name])
                    results[name] = {'source': 'Stooq', 'unit': unit, 'series': series}
                    print(f'    stooq {name}: {len(series)} Punkte')
                except Exception as e2:
                    print(f'  ! Stooq {name}: {e2}')
                    results[name] = {'source': '', 'unit': unit, 'series': []}
            else:
                results[name] = {'source': '', 'unit': unit, 'series': []}

    # EU ETS CO2 – Ember Climate (CSV, öffentlich)
    try:
        r = SESSION.get(
            'https://ember-climate.org/app/uploads/2022/03/Carbon-Price-Viewer.csv',
            timeout=20
        )
        if r.status_code == 200:
            lines = r.text.strip().split('\n')
            series = []
            for line in lines[1:]:
                parts = line.split(',')
                if len(parts) >= 2:
                    try:
                        series.append({'date': parts[0].strip(), 'p': float(parts[1].strip())})
                    except:
                        pass
            results['eu_ets_co2'] = {'source': 'Ember Climate', 'unit': 'EUR/tCO2', 'series': series}
            print(f'    ember CO2: {len(series)} Punkte')
    except Exception as e:
        print(f'  ! Ember CO2: {e}')

    # Benzinpreise Europa – GlobalPetrolPrices (öffentliche JSON-Daten)
    try:
        r = SESSION.get(
            'https://www.globalpetrolprices.com/gasoline_prices/',
            timeout=15,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        # Diese Seite gibt HTML zurück, kein JSON – nur als Referenz
        # Wir nutzen stattdessen den Tankerkoenig für DE
        results['fuel_note'] = 'Tankerkoenig fuer DE, GlobalPetrolPrices als Referenz'
    except:
        pass

    save('commodities', {'updated': now_utc().isoformat(), **results})

# ════════════════════════════════════════════════════════
# 8. OPEN POWER SYSTEM DATA – Bundesland-Kapazitäten
#    https://open-power-system-data.org
# ════════════════════════════════════════════════════════
def fetch_bundesland():
    results = {}

    # OPSD Renewable Power Plants DE
    try:
        url = 'https://data.open-power-system-data.org/renewable_power_plants/latest/renewable_power_plants_DE.csv'
        r = SESSION.get(url, timeout=90, stream=True)
        content = b''
        for chunk in r.iter_content(chunk_size=131072):
            content += chunk
            if len(content) > 8 * 1024 * 1024:
                break

        lines = content.decode('utf-8', errors='replace').split('\n')
        header = [h.strip().strip('"').lower() for h in lines[0].split(',')]

        def col(*names):
            for n in names:
                if n in header:
                    return header.index(n)
            return -1

        state_idx    = col('federal_state', 'bundesland', 'state', 'land')
        type_idx     = col('energy_source_level_2', 'energy_source', 'energietraeger', 'type')
        cap_idx      = col('electrical_capacity', 'capacity_net_bnetza', 'nettonennleistung', 'capacity')
        year_idx     = col('commissioning_date', 'start_up_date', 'inbetriebnahmedatum')

        if state_idx < 0 or cap_idx < 0:
            raise ValueError(f'Spalten nicht gefunden. Header: {header[:20]}')

        # Aggregieren nach Bundesland + Technologie + Jahr
        agg = {}  # {state: {type: {year: MW}}}
        for line in lines[1:]:
            parts = line.split(',')
            if len(parts) <= max(state_idx, cap_idx):
                continue
            try:
                state = parts[state_idx].strip().strip('"').strip()
                etype = parts[type_idx].strip().strip('"') if type_idx >= 0 else 'Unbekannt'
                cap   = float(parts[cap_idx].strip().replace('"', '') or 0)
                year  = parts[year_idx][:4] if year_idx >= 0 and len(parts) > year_idx else 'unbekannt'
                if not state or cap <= 0:
                    continue
                if state not in agg:
                    agg[state] = {}
                if etype not in agg[state]:
                    agg[state][etype] = {}
                agg[state][etype][year] = agg[state][etype].get(year, 0) + cap / 1000  # MW → GW
            except (ValueError, IndexError):
                pass

        # Aufräumen: auf 3 Stellen runden
        clean = {}
        for state, types in agg.items():
            clean[state] = {t: {yr: round(v, 3) for yr, v in yrs.items()} for t, yrs in types.items()}
        results['by_bundesland'] = clean
        results['source'] = 'OPSD – Open Power System Data'
        results['row_count'] = len(lines)
    except Exception as e:
        print(f'  ! OPSD: {e}')
        results['by_bundesland'] = {}
        results['opsd_error'] = str(e)

    # Fallback national via Energy-Charts
    try:
        r = SESSION.get(f'{EC}/installed_power', params={'country': 'de', 'time_step': 'yearly'}, timeout=20)
        r.raise_for_status()
        results['national_ec'] = r.json()
    except Exception as e:
        print(f'  ! EC national: {e}')

    save('bundesland', {'updated': now_utc().isoformat(), **results})

# ════════════════════════════════════════════════════════
# 9. SPOT HISTORY – 20 Jahre monatliche Preise
#    Energy-Charts Fraunhofer ISE
# ════════════════════════════════════════════════════════
def fetch_spot_history():
    results = {}
    end_str = now_utc().strftime('%Y-%m-%d')

    # Monatliche Preise pro Land seit 2005
    zones = [
        ('DE-LU',    'de', '2018-10-01', end_str),
        ('DE-AT-LU', 'de_old', '2005-01-01', '2018-09-30'),
        ('FR',       'fr', '2005-01-01', end_str),
        ('AT',       'at', '2005-01-01', end_str),
        ('ES',       'es', '2005-01-01', end_str),
        ('IT-North', 'it', '2010-01-01', end_str),
        ('NL',       'nl', '2010-01-01', end_str),
        ('PL',       'pl', '2010-01-01', end_str),
    ]

    for bzn, key, start, end in zones:
        try:
            r = SESSION.get(f'{EC}/price', params={
                'bzn': bzn, 'start': start, 'end': end, 'interval': 'month'
            }, timeout=35)
            r.raise_for_status()
            d = r.json()
            results[f'monthly_{key}'] = {
                'bzn': bzn,
                'unix_seconds': d.get('unix_seconds', []),
                'price': d.get('price', []),
                'unit': 'EUR/MWh'
            }
            print(f'    spot history {bzn}: {len(d.get("price",[]))} Monate')
            time.sleep(0.2)
        except Exception as e:
            print(f'  ! spot {bzn}: {e}')

    # Jährliche Durchschnitte DE
    try:
        r = SESSION.get(f'{EC}/price', params={
            'bzn': 'DE-LU', 'start': '2005-01-01', 'end': end_str, 'interval': 'year'
        }, timeout=30)
        r.raise_for_status()
        d = r.json()
        results['yearly_de'] = {
            'unix_seconds': d.get('unix_seconds', []),
            'price': d.get('price', []),
            'unit': 'EUR/MWh'
        }
    except Exception as e:
        print(f'  ! yearly DE: {e}')

    save('spot_history', {'updated': now_utc().isoformat(), 'source': 'Fraunhofer ISE / Energy-Charts', **results})

# ════════════════════════════════════════════════════════
# 10. METADATA
# ════════════════════════════════════════════════════════
def write_meta():
    save('meta', {
        'last_fetch': now_utc().isoformat(),
        'next_fetch_approx': (now_utc() + timedelta(minutes=15)).isoformat(),
        'version': '3.0',
        'sources': {
            'smard':         'smard.de – Bundesnetzagentur (15-min, korrekte URL mit /DE/)',
            'energy_charts': 'api.energy-charts.info – Fraunhofer ISE (Preise, Erzeugung, CO2, Gas, Kohle)',
            'gas_storage':   'agsi.gie.eu – GIE AGSI+ (10 Länder)',
            'macro':         'data-api.ecb.europa.eu – EZB (HICP 8 Länder + FX)',
            'weather':       'api.open-meteo.com – 15 Städte, kein API-Key',
            'fuel':          'creativecommons.tankerkoenig.de – 20 Städte DE',
            'commodities':   'Yahoo Finance + Stooq (Fallback) + Ember Climate (CO2)',
            'bundesland':    'open-power-system-data.org – OPSD (nach Bundesland + Jahr)',
            'spot_history':  'api.energy-charts.info – monatlich ab 2005, 8 Länder',
        }
    })

# ════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════
if __name__ == '__main__':
    print(f'=== Energy Dashboard Fetch v3 ===')
    print(f'Start: {now_utc().isoformat()}\n')

    steps = [
        ('SMARD (korrekte URLs)',              fetch_smard),
        ('SMARD History 20yr',                fetch_smard_history),
        ('Energy-Charts Fraunhofer',          fetch_energy_charts),
        ('AGSI+ Gas Storage 12 Laender',      fetch_agsi),
        ('ECB Inflation + FX',                fetch_ecb),
        ('Open-Meteo 15 Staedte',             fetch_weather),
        ('Tankerkoenig 20 Staedte',           fetch_tankerkoenig),
        ('Commodities Yahoo+Stooq+Ember',     fetch_commodities),
        ('Bundesland OPSD',                   fetch_bundesland),
        ('Spot History 20yr 8 Laender',       fetch_spot_history),
        ('Metadata',                          write_meta),
    ]

    for label, fn in steps:
        print(f'[{label}]')
        try:
            fn()
        except Exception as e:
            print(f'  !! FEHLER: {e}')
        print()

    print(f'=== Fertig: {now_utc().isoformat()} ===')
```

---

## `requirements.txt` (neue Datei!)
```
requests
pytz
