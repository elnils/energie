"""
Energy Dashboard – Fetcher v4.2
Fixes: AGSI+ field names, Tankerkoenig key check, News feeds updated,
       Energy-Charts field name normalization with debug output
"""
import json, os, re, time, requests, pytz
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

OUT = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(OUT, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 EnergyDashboard/4.2',
    'Accept': 'application/json, */*',
})

def save(name, data):
    path = os.path.join(OUT, f'{name}.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
    print(f'  ✓ {name}.json ({os.path.getsize(path)//1024} KB)')

def now_utc():
    return datetime.now(timezone.utc)

def sf(v, default=0.0):
    try: return float(v or default)
    except: return default

# ════════════════════════════════
# 1. SMARD
# ════════════════════════════════
SMARD_FILTERS = {
    'wind_onshore': 4067, 'wind_offshore': 1225, 'solar': 4068,
    'biomass': 4066, 'hydro': 1226, 'nuclear': 1224,
    'lignite': 1223, 'hard_coal': 4069, 'natural_gas': 4071,
    'load': 410,
}

def fetch_smard():
    results = {}
    berlin = pytz.timezone('Europe/Berlin')
    now_b = datetime.now(berlin)
    mon_ms = int((now_b - timedelta(days=now_b.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    prev_mon_ms = mon_ms - 7 * 24 * 3600 * 1000

    for name, fid in SMARD_FILTERS.items():
        series = []
        try:
            r = SESSION.get(f'https://www.smard.de/app/chart_data/{fid}/DE/index_quarterhour.json', timeout=20)
            r.raise_for_status()
            timestamps = r.json().get('timestamps', [])
            recent = [t for t in timestamps if t >= prev_mon_ms] or timestamps[-2:]
            seen = set()
            for bucket in recent[:2]:
                try:
                    url2 = f'https://www.smard.de/app/chart_data/{fid}/DE/{fid}_DE_quarterhour_{bucket}.json'
                    raw = SESSION.get(url2, timeout=20).json().get('series', [])
                    for e in raw:
                        if isinstance(e, list) and len(e)==2 and e[1] is not None and e[0] not in seen:
                            seen.add(e[0])
                            series.append({'ts': e[0], 'v': round(float(e[1]), 2)})
                    time.sleep(0.05)
                except Exception as ex:
                    print(f'      bucket {bucket}: {ex}')
            series.sort(key=lambda x: x['ts'])
            print(f'    smard/{name}: {len(series)} pts')
        except Exception as e:
            print(f'  ! smard/{name}: {e}')
        results[name] = series

    save('smard', {'updated': now_utc().isoformat(), 'series': results})

# ════════════════════════════════
# 2. ENERGY-CHARTS
# ════════════════════════════════
EC = 'https://api.energy-charts.info'

def ec_get(path, params=None):
    r = SESSION.get(f'{EC}/{path}', params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def pick(d, *keys, default=None):
    """Pick first non-empty value from dict by trying multiple keys."""
    for k in keys:
        v = d.get(k)
        if v is not None and v != [] and v != {}:
            return v
    return default if default is not None else []

def fetch_energy_charts():
    results = {}
    end = now_utc()
    end_str = end.strftime('%Y-%m-%d')

    # Strompreise
    for bzn, key in [('DE-LU','price_de'),('AT','price_at'),('FR','price_fr'),('PL','price_pl'),('CH','price_ch')]:
        try:
            d = ec_get('price', {'bzn': bzn, 'start': 'P7D'})
            results[key] = {'unix_seconds': d.get('unix_seconds',[]), 'price': d.get('price',[]), 'unit':'EUR/MWh'}
            print(f'    ec/{key}: {len(d.get("price",[]))} pts')
            time.sleep(0.15)
        except Exception as e:
            print(f'  ! ec/{key}: {e}')
            results[key] = {'unix_seconds':[], 'price':[], 'unit':'EUR/MWh'}

    # Monatliche Preise DE (20 Jahre)
    try:
        d1 = ec_get('price', {'bzn':'DE-LU','start':'2018-10-01','end':end_str,'interval':'month'})
        d2 = ec_get('price', {'bzn':'DE-AT-LU','start':'2005-01-01','end':'2018-09-30','interval':'month'})
        results['price_de_monthly'] = {
            'unix_seconds': d2.get('unix_seconds',[]) + d1.get('unix_seconds',[]),
            'price': d2.get('price',[]) + d1.get('price',[]),
        }
        print(f'    ec/price_monthly: {len(results["price_de_monthly"]["price"])} pts')
        time.sleep(0.2)
    except Exception as e:
        print(f'  ! ec/monthly: {e}')
        results['price_de_monthly'] = {'unix_seconds':[], 'price':[]}

    # Öffentliche Erzeugung DE
    try:
        d = ec_get('public_power', {
            'country': 'de',
            'start': (end - timedelta(days=7)).strftime('%Y-%m-%dT%H:%MZ'),
            'end': end.strftime('%Y-%m-%dT%H:%MZ'),
        })
        results['public_power_de'] = d
        print(f'    ec/public_power: keys={list(d.keys())}')
        time.sleep(0.2)
    except Exception as e:
        print(f'  ! ec/public_power: {e}')

    # EE-Anteil
    try:
        d = ec_get('ren_share_in_public_power', {'country':'de','start':'P30D'})
        print(f'    ec/ren_share raw keys: {list(d.keys())}')
        ren_vals = pick(d, 'share_of_generation_capacity', 'ren_share', 'renewable_share', 'ren_share_in_public_power')
        results['ren_share_de'] = {
            'unix_seconds': d.get('unix_seconds', []),
            'ren_share': ren_vals,
        }
        print(f'    ec/ren_share: {len(results["ren_share_de"]["unix_seconds"])} pts')
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/ren_share: {e}')
        results['ren_share_de'] = {'unix_seconds':[], 'ren_share':[]}

    # Installierte Leistung
    try:
        d = ec_get('installed_power', {'country':'de','time_step':'yearly'})
        results['installed_de'] = d
        print(f'    ec/installed_de: keys={list(d.keys())}')
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/installed_de: {e}')

    # TTF Gas
    try:
        d = ec_get('gas_price', {'start':'P730D'})
        print(f'    ec/gas_price raw keys: {list(d.keys())}')
        gas_vals = pick(d, 'Gas Price', 'price', 'gas_price', 'value')
        gas_ts   = pick(d, 'unix_seconds', 'timestamp', 'time')
        results['gas_price'] = {'unix_seconds': gas_ts, 'price': gas_vals}
        print(f'    ec/gas_price: {len(gas_ts)} pts')
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/gas_price: {e}')
        results['gas_price'] = {'unix_seconds':[], 'price':[]}

    # CO2 Preis
    try:
        d = ec_get('co2_price', {'start':'P730D'})
        print(f'    ec/co2_price raw keys: {list(d.keys())}')
        co2_vals = pick(d, 'CO2 Price', 'co2_price', 'price', 'value')
        co2_ts   = pick(d, 'unix_seconds', 'timestamp', 'time')
        results['co2_price'] = {'unix_seconds': co2_ts, 'price': co2_vals}
        print(f'    ec/co2_price: {len(co2_ts)} pts')
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/co2_price: {e}')
        results['co2_price'] = {'unix_seconds':[], 'price':[]}

    # Grenzüberschreitender Handel
    try:
        d = ec_get('cross_border_electricity_trading', {'country':'de','start':'P7D'})
        print(f'    ec/cross_border raw keys: {list(d.keys())}')
        # Dump small sample to see structure
        for k, v in d.items():
            sample = v[:2] if isinstance(v, list) and len(v) > 2 else v
            print(f'      {k}: {sample}')
        net  = pick(d, 'net', 'Net', 'cross_border_de', 'total')
        ts   = pick(d, 'unix_seconds', 'timestamp', 'time')
        results['cross_border_de'] = {'unix_seconds': ts, 'net': net, '_all_keys': list(d.keys())}
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/cross_border: {e}')
        results['cross_border_de'] = {'unix_seconds':[], 'net':[]}

    save('energy_charts', {'updated': now_utc().isoformat(), **results})

# ════════════════════════════════
# 3. AGSI+ Gasspeicher
# ════════════════════════════════
def fetch_gas():
    results = {}
    countries = {
        'eu':'EU gesamt','de':'Deutschland','at':'Österreich',
        'fr':'Frankreich','it':'Italien','nl':'Niederlande',
        'be':'Belgien','pl':'Polen','es':'Spanien'
    }

    # First: dump one raw entry to understand the structure
    try:
        r = SESSION.get('https://agsi.gie.eu/api', params={'country': 'de', 'size': 3}, timeout=25)
        r.raise_for_status()
        payload = r.json()
        raw_sample = payload.get('data', payload) if isinstance(payload, dict) else payload
        if raw_sample and isinstance(raw_sample, list):
            print(f'    AGSI raw sample entry: {json.dumps(raw_sample[0], ensure_ascii=False)[:500]}')
    except Exception as e:
        print(f'  ! AGSI sample: {e}')

    for code, name in countries.items():
        try:
            r = SESSION.get('https://agsi.gie.eu/api', params={'country': code, 'size': 60}, timeout=25)
            r.raise_for_status()
            payload = r.json()
            raw = payload.get('data', payload) if isinstance(payload, dict) else payload
            if not isinstance(raw, list):
                print(f'    agsi/{code}: unexpected format: {type(raw)}')
                results[code] = {'name': name, 'data': []}
                continue

            cleaned = []
            for entry in raw:
                # ── Date ──
                date = ''
                for df in ['gasDayStart', 'date', 'reportingPeriod', 'datetime']:
                    v = str(entry.get(df) or '')
                    if v and len(v) >= 10:
                        date = v[:10]; break

                # ── Fill percentage ──
                # GIE API uses: full (%), gasInStorage (TWh), trend (%), status (%)
                fill = None

                # Direct percentage fields
                for fld in ['full', 'trend', 'status', 'full_is_percentage', 'fillLevelFull']:
                    v = entry.get(fld)
                    if v is not None and v != '' and v != 'NaN':
                        try:
                            fv = float(str(v).replace(',', '.'))
                            if 0 <= fv <= 100:  # must be a percentage
                                fill = round(fv, 1)
                                break
                        except: pass

                # If still None: gasInStorage / workingGasVolume as % via capacity
                if fill is None:
                    stored = None
                    cap = None
                    for sf_fld in ['gasInStorage', 'workingGasVolume', 'currentStorage']:
                        v = entry.get(sf_fld)
                        if v is not None and v != '':
                            try: stored = float(str(v).replace(',','.')); break
                            except: pass
                    for cf_fld in ['workingGasVolume', 'full', 'capacity', 'totalStorage']:
                        v = entry.get(cf_fld)
                        if v is not None and v != '' and cf_fld != sf_fld:
                            try: cap = float(str(v).replace(',','.')); break
                            except: pass
                    if stored and cap and cap > 0:
                        fill = round(stored / cap * 100, 1)

                # ── Injection / Withdrawal ──
                def get_float(entry, *fields):
                    for f in fields:
                        v = entry.get(f)
                        if v is not None and v != '' and v != 'NaN':
                            try: return float(str(v).replace(',','.'))
                            except: pass
                    return 0.0

                inj = get_float(entry, 'injection', 'ins', 'inflow', 'injectionCapacity')
                con = get_float(entry, 'withdrawal', 'con', 'outflow', 'consumption', 'withdrawalCapacity')

                if date:
                    cleaned.append({
                        'date': date,
                        'fill_pct': fill,
                        'injection': round(inj, 3),
                        'withdrawal': round(con, 3),
                    })

            cleaned.sort(key=lambda x: x['date'])
            last_entry = cleaned[-1] if cleaned else {}
            print(f'    agsi/{code}: {len(cleaned)} entries, last={last_entry}')
            results[code] = {'name': name, 'data': cleaned}
            time.sleep(0.2)

        except Exception as e:
            print(f'  ! agsi/{code}: {e}')
            results[code] = {'name': name, 'data': []}

    save('gas_storage', {'updated': now_utc().isoformat(), **results})

# ════════════════════════════════
# 4. TANKERKOENIG
# ════════════════════════════════
FUEL_CITIES = {
    'Berlin':     (52.520, 13.405), 'Hamburg':    (53.550, 10.000),
    'München':    (48.137, 11.575), 'Köln':       (50.938,  6.960),
    'Frankfurt':  (50.110,  8.682), 'Stuttgart':  (48.775,  9.182),
    'Düsseldorf': (51.227,  6.773), 'Leipzig':    (51.340, 12.375),
    'Nürnberg':   (49.452, 11.077), 'Dortmund':   (51.514,  7.465),
    'Bremen':     (53.079,  8.801), 'Hannover':   (52.375,  9.735),
    'Dresden':    (51.050, 13.740), 'Bonn':       (50.733,  7.100),
    'Augsburg':   (48.371, 10.898), 'Wiesbaden':  (50.082,  8.240),
    'Bielefeld':  (51.978,  8.532), 'Münster':    (51.962,  7.628),
}

def fetch_fuel():
    api_key = os.environ.get('TANKERKOENIG_API_KEY', '').strip()
    if not api_key:
        print('  ! Tankerkoenig: TANKERKOENIG_API_KEY nicht gesetzt!')
        save('fuel', {'updated': now_utc().isoformat(), 'error': 'no_api_key', 'cities': {}})
        return
    print(f'    API-Key: {api_key[:8]}…')

    def avg(lst): return round(sum(lst)/len(lst), 3) if lst else None

    def get_stations(lat, lon, rad=10, fuel_type='e5'):
        """Fetch stations for a specific fuel type (type != all avoids sort restriction)."""
        r = SESSION.get(
            'https://creativecommons.tankerkoenig.de/json/list.php',
            params={'lat': lat, 'lng': lon, 'rad': rad, 'sort': 'price', 'type': fuel_type, 'apikey': api_key},
            timeout=15
        )
        r.raise_for_status()
        d = r.json()
        if not d.get('ok'):
            raise ValueError(d.get('message', 'api_error'))
        return d.get('stations', [])

    results = {}
    for city, (lat, lon) in FUEL_CITIES.items():
        try:
            city_data = {'count': 0}
            for fuel_type, key in [('e5', 'e5'), ('e10', 'e10'), ('diesel', 'diesel')]:
                try:
                    stations = get_stations(lat, lon, rad=10, fuel_type=fuel_type)
                    prices = sorted([s[fuel_type] for s in stations
                                     if isinstance(s.get(fuel_type), float) and s[fuel_type] > 0.5])
                    city_data[f'{key}_avg'] = avg(prices)
                    city_data[f'{key}_min'] = prices[0] if prices else None
                    city_data['count'] = max(city_data['count'], len(stations))
                    time.sleep(0.2)
                except Exception as e2:
                    print(f'      {city}/{fuel_type}: {e2}')
            results[city] = city_data
            print(f'    fuel/{city}: E5={city_data.get("e5_avg")}, E10={city_data.get("e10_avg")}, Diesel={city_data.get("diesel_avg")}')
            time.sleep(0.1)
        except Exception as e:
            print(f'  ! fuel/{city}: {e}')

    # Nationaler Durchschnitt
    nat = {'count': 0}
    for fuel_type in ['e5', 'e10', 'diesel']:
        try:
            stations = get_stations(51.163, 10.447, rad=150, fuel_type=fuel_type)
            prices = [s[fuel_type] for s in stations
                      if isinstance(s.get(fuel_type), float) and s[fuel_type] > 0.5]
            nat[f'{fuel_type}_avg'] = avg(prices)
            nat['count'] = max(nat['count'], len(stations))
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! fuel/national/{fuel_type}: {e}')
    results['_national'] = nat

    save('fuel', {'updated': now_utc().isoformat(), 'cities': results})

# ════════════════════════════════
# 5. WETTER
# ════════════════════════════════
WEATHER_CITIES = {
    'Berlin':(52.52,13.41),'Hamburg':(53.55,10.00),
    'München':(48.14,11.58),'Frankfurt':(50.11,8.68),
    'Köln':(50.94,6.96),'Stuttgart':(48.78,9.18),
    'Düsseldorf':(51.22,6.77),'Leipzig':(51.34,12.38),
}

def fetch_weather():
    results = {}
    for city, (lat, lon) in WEATHER_CITIES.items():
        try:
            r = SESSION.get('https://api.open-meteo.com/v1/forecast', params={
                'latitude':lat,'longitude':lon,
                'current':'temperature_2m,wind_speed_10m,direct_radiation,relative_humidity_2m',
                'hourly':'temperature_2m,wind_speed_100m,direct_radiation,cloud_cover,precipitation_probability',
                'daily':'temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,shortwave_radiation_sum',
                'forecast_days':5,'timezone':'Europe/Berlin'
            }, timeout=15)
            r.raise_for_status()
            results[city] = r.json()
            time.sleep(0.07)
        except Exception as e:
            print(f'  ! weather/{city}: {e}')
    save('weather', {'updated': now_utc().isoformat(), 'cities': results})

# ════════════════════════════════
# 6. ROHSTOFFE
# ════════════════════════════════
TICKERS = {
    'brent_crude':  ('BZ=F','USD/Barrel'),
    'wti_crude':    ('CL=F','USD/Barrel'),
    'natgas_henry': ('NG=F','USD/MMBtu'),
    'gold':         ('GC=F','USD/oz'),
    'coal':         ('MTF=F','USD/t'),
}

def fetch_commodities():
    results = {}
    for name, (ticker, unit) in TICKERS.items():
        try:
            enc = ticker.replace('=','%3D')
            r = SESSION.get(
                f'https://query1.finance.yahoo.com/v8/finance/chart/{enc}?range=2y&interval=1d',
                timeout=20,
                headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64)','Referer':'https://finance.yahoo.com'}
            )
            r.raise_for_status()
            res = r.json()['chart']['result'][0]
            ts = res['timestamp']
            closes = res['indicators']['quote'][0]['close']
            series = [{'ts':t,'v':round(c,4)} for t,c in zip(ts,closes) if c is not None]
            results[name] = {'unit':unit,'series':series}
            print(f'    commodity/{name}: {len(series)} pts')
            time.sleep(0.4)
        except Exception as e:
            print(f'  ! commodity/{name}: {e}')
            results[name] = {'unit':unit,'series':[]}
    save('commodities', {'updated': now_utc().isoformat(), **results})

# ════════════════════════════════
# 7. NEWS
# ════════════════════════════════
RSS_FEEDS = [
    ('PV Magazine',     'https://www.pv-magazine.de/feed/'),
    ('Energie Zukunft', 'https://www.energiezukunft.eu/feed/'),
    ('Solar Server',    'https://www.solarserver.de/feed/'),
    ('IWR',             'https://www.iwr.de/uploads/tx_vxnewsrss/iwr_rss_feed.xml'),
    ('BNetzA',          'https://www.bundesnetzagentur.de/SiteGlobals/Functions/RSS/DE/RSS-Newsfeed.xml'),
    ('BDEW',            'https://www.bdew.de/service/pressemitteilungen/?format=feed&type=rss'),
    ('Klimareporter',   'https://www.klimareporter.de/feed'),
    ('Heise Energie',   'https://www.heise.de/thema/Energiewende/feed/atom.xml'),
]

def strip_html(text):
    return re.sub(r'<[^>]+>', '', text or '').strip()

def clean_xml(content_bytes):
    text = content_bytes.decode('utf-8', errors='replace')
    # Fix unescaped ampersands
    text = re.sub(r'&(?!(amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)', '&amp;', text)
    return text.encode('utf-8')

def fetch_news():
    articles = []
    ATOM = 'http://www.w3.org/2005/Atom'
    for source, url in RSS_FEEDS:
        try:
            r = SESSION.get(url, timeout=12, headers={
                'Accept':'application/rss+xml,application/xml,text/xml,*/*',
                'User-Agent':'Mozilla/5.0',
            })
            r.raise_for_status()
            try:
                root = ET.fromstring(r.content)
            except ET.ParseError:
                root = ET.fromstring(clean_xml(r.content))

            channel = root.find('channel')
            items = channel.findall('item') if channel is not None else []
            if not items:
                items = root.findall(f'{{{ATOM}}}entry')

            count = 0
            for item in items[:8]:
                title = strip_html(
                    item.findtext('title','') or
                    item.findtext(f'{{{ATOM}}}title','')
                )
                link_el = item.find(f'{{{ATOM}}}link')
                link = (
                    (link_el.get('href','') if link_el is not None else '') or
                    (item.findtext('link','') or '').strip()
                )
                pubdate = (
                    item.findtext('pubDate','') or
                    item.findtext(f'{{{ATOM}}}published','') or
                    item.findtext(f'{{{ATOM}}}updated','')
                )
                desc = strip_html(
                    item.findtext('description','') or
                    item.findtext(f'{{{ATOM}}}summary','')
                )[:280]
                if title and link:
                    articles.append({'source':source,'title':title,'link':link,'date':pubdate,'summary':desc})
                    count += 1
            print(f'    news/{source}: {count} Artikel')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! news/{source}: {e}')

    save('news', {'updated': now_utc().isoformat(), 'articles': articles[:120]})

# ════════════════════════════════
# 8. META
# ════════════════════════════════
def write_meta():
    save('meta', {
        'last_fetch': now_utc().isoformat(),
        'next_fetch_approx': (now_utc() + timedelta(minutes=15)).isoformat(),
        'version': '4.2',
    })

if __name__ == '__main__':
    print(f'=== Energy Dashboard Fetch v4.2 – {now_utc().isoformat()} ===\n')
    for label, fn in [
        ('SMARD',         fetch_smard),
        ('Energy-Charts', fetch_energy_charts),
        ('AGSI+ Gas',     fetch_gas),
        ('Tankerkoenig',  fetch_fuel),
        ('Wetter',        fetch_weather),
        ('Rohstoffe',     fetch_commodities),
        ('News',          fetch_news),
        ('Meta',          write_meta),
    ]:
        print(f'[{label}]')
        try: fn()
        except Exception as e: print(f'  !! FEHLER: {e}')
        print()
    print(f'=== Fertig: {now_utc().isoformat()} ===')
