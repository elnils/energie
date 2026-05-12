"""
Energy Dashboard – Fetcher v4.3
Fixes: Tankerkoenig key cleaning, AGSI+ EIC codes, Energy-Charts field normalization
"""
import json, os, re, time, requests, pytz
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

OUT = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(OUT, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 EnergyDashboard/4.3',
    'Accept': 'application/json, */*',
})

def save(name, data):
    path = os.path.join(OUT, f'{name}.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
    print(f'  v {name}.json ({os.path.getsize(path)//1024} KB)')

def now_utc():
    return datetime.now(timezone.utc)

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

def first_list(d, *keys):
    for k in keys:
        v = d.get(k)
        if isinstance(v, list) and len(v) > 0:
            return v
    return []

def fetch_energy_charts():
    results = {}
    end = now_utc()
    end_str = end.strftime('%Y-%m-%d')

    for bzn, key in [('DE-LU','price_de'),('AT','price_at'),('FR','price_fr'),('PL','price_pl'),('CH','price_ch')]:
        try:
            d = ec_get('price', {'bzn': bzn, 'start': 'P7D'})
            results[key] = {'unix_seconds': d.get('unix_seconds',[]), 'price': d.get('price',[]), 'unit':'EUR/MWh'}
            print(f'    ec/{key}: {len(d.get("price",[]))} pts')
            time.sleep(0.15)
        except Exception as e:
            print(f'  ! ec/{key}: {e}')
            results[key] = {'unix_seconds':[], 'price':[], 'unit':'EUR/MWh'}

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

    try:
        d = ec_get('public_power', {
            'country': 'de',
            'start': (end - timedelta(days=7)).strftime('%Y-%m-%dT%H:%MZ'),
            'end': end.strftime('%Y-%m-%dT%H:%MZ'),
        })
        results['public_power_de'] = d
        time.sleep(0.2)
    except Exception as e:
        print(f'  ! ec/public_power: {e}')

    try:
        d = ec_get('ren_share_in_public_power', {'country':'de','start':'P30D'})
        print(f'    ec/ren_share keys: {list(d.keys())}')
        results['ren_share_de'] = {
            'unix_seconds': d.get('unix_seconds', []),
            'ren_share': first_list(d, 'share_of_generation_capacity','ren_share','renewable_share','ren_share_in_public_power'),
        }
        print(f'    ec/ren_share: {len(results["ren_share_de"]["unix_seconds"])} pts')
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/ren_share: {e}')
        results['ren_share_de'] = {'unix_seconds':[], 'ren_share':[]}

    try:
        d = ec_get('installed_power', {'country':'de','time_step':'yearly'})
        results['installed_de'] = d
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/installed_de: {e}')

    try:
        d = ec_get('gas_price', {'start':'P730D'})
        print(f'    ec/gas_price keys: {list(d.keys())}')
        results['gas_price'] = {
            'unix_seconds': first_list(d, 'unix_seconds','timestamp','time'),
            'price': first_list(d, 'Gas Price','price','gas_price','value','data'),
        }
        print(f'    ec/gas_price: {len(results["gas_price"]["unix_seconds"])} pts')
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/gas_price: {e}')
        results['gas_price'] = {'unix_seconds':[], 'price':[]}

    try:
        d = ec_get('co2_price', {'start':'P730D'})
        print(f'    ec/co2_price keys: {list(d.keys())}')
        results['co2_price'] = {
            'unix_seconds': first_list(d, 'unix_seconds','timestamp','time'),
            'price': first_list(d, 'CO2 Price','co2_price','price','value','data'),
        }
        print(f'    ec/co2_price: {len(results["co2_price"]["unix_seconds"])} pts')
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/co2_price: {e}')
        results['co2_price'] = {'unix_seconds':[], 'price':[]}

    try:
        d = ec_get('cross_border_electricity_trading', {'country':'de','start':'P7D'})
        print(f'    ec/cross_border keys: {list(d.keys())}')
        # Dump ALL keys with types/lengths
        for k, v in d.items():
            if isinstance(v, list):
                sample = v[:2]
                print(f'      {k}[{len(v)}]: {sample}')
        results['cross_border_de'] = d  # store entire response, JS will handle it
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/cross_border: {e}')
        results['cross_border_de'] = {}

    save('energy_charts', {'updated': now_utc().isoformat(), **results})

# ════════════════════════════════
# 3. AGSI+ Gasspeicher
# IMPORTANT: AGSI uses EIC codes, not ISO country codes for some countries
# EU aggregate = 'eu', countries by ISO2
# ════════════════════════════════
def fetch_gas():
    results = {}
    # AGSI+ uses these exact country parameters
    countries = {
        'eu': 'EU gesamt',
        'de': 'Deutschland',
        'at': 'Österreich',
        'fr': 'Frankreich',
        'it': 'Italien',
        'nl': 'Niederlande',
        'be': 'Belgien',
        'pl': 'Polen',
        'es': 'Spanien',
    }

    # Print FULL raw entry for one country to see exact field names
    for debug_cc in ['de', 'eu']:
        try:
            r = SESSION.get('https://agsi.gie.eu/api', params={'country': debug_cc, 'size': 2}, timeout=25)
            r.raise_for_status()
            payload = r.json()
            top_keys = list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__
            print(f'    AGSI {debug_cc} top keys: {top_keys}')
            raw_s = payload.get('data', payload) if isinstance(payload, dict) else payload
            if isinstance(raw_s, list) and raw_s:
                print(f'    AGSI {debug_cc} entry[0]: {json.dumps(raw_s[0])}')
            elif isinstance(payload, dict):
                print(f'    AGSI {debug_cc} full response: {json.dumps(payload)[:600]}')
        except Exception as e:
            print(f'  ! AGSI debug {debug_cc}: {e}')

    for code, name in countries.items():
        try:
            r = SESSION.get('https://agsi.gie.eu/api', params={'country': code, 'size': 60}, timeout=25)
            r.raise_for_status()
            payload = r.json()

            # Handle both {data: [...]} and [...] responses
            if isinstance(payload, dict) and 'data' in payload:
                raw = payload['data']
            elif isinstance(payload, list):
                raw = payload
            elif isinstance(payload, dict):
                # Maybe it's {status, data, ...} or the entries are direct
                raw = payload.get('data') or payload.get('result') or payload.get('entries') or []
            else:
                raw = []

            if not isinstance(raw, list):
                raw = []

            print(f'    agsi/{code}: {len(raw)} raw entries')

            cleaned = []
            for entry in raw:
                if not isinstance(entry, dict):
                    continue

                # Date
                date = ''
                for df in ['gasDayStart', 'date', 'reportingPeriod', 'datetime', 'period']:
                    v = str(entry.get(df) or '')
                    if len(v) >= 10:
                        date = v[:10]
                        break

                # Fill % - try every conceivable field name
                fill = None
                for fld in ['full', 'trend', 'status', 'full_is_percentage',
                             'fillLevelFull', 'gasInStoragePercent', 'percentFull']:
                    v = entry.get(fld)
                    if v is not None and str(v) not in ('', 'NaN', 'null', 'None'):
                        try:
                            fv = float(str(v).replace(',', '.'))
                            if 0.0 <= fv <= 100.0:
                                fill = round(fv, 2)
                                break
                        except:
                            pass

                # Fallback: gasInStorage / workingGasVolume ratio
                if fill is None:
                    try:
                        stored = float(str(entry.get('gasInStorage') or '0').replace(',','.'))
                        wgv = float(str(entry.get('workingGasVolume') or '0').replace(',','.'))
                        if wgv > 0:
                            fill = round(stored / wgv * 100, 2)
                    except:
                        pass

                def gf(entry, *fields):
                    for f in fields:
                        v = entry.get(f)
                        if v is not None and str(v) not in ('', 'NaN', 'null'):
                            try: return round(float(str(v).replace(',','.')), 4)
                            except: pass
                    return 0.0

                inj = gf(entry, 'injection', 'ins', 'inflow')
                con = gf(entry, 'withdrawal', 'con', 'outflow', 'consumption')

                if date:
                    cleaned.append({
                        'date': date,
                        'fill_pct': fill,
                        'injection': inj,
                        'withdrawal': con,
                    })

            cleaned.sort(key=lambda x: x['date'])
            if cleaned:
                print(f'    agsi/{code}: {len(cleaned)} cleaned, last={cleaned[-1]}')
            else:
                print(f'    agsi/{code}: 0 cleaned (raw had {len(raw)} entries)')
                if raw:
                    print(f'      first raw entry: {json.dumps(raw[0])[:300]}')

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
    print(f'    API-Key: {api_key[:8]}... (len={len(api_key)})')
    if len(api_key) < 10:
        print('  ! API-Key fehlt')
        save('fuel', {'updated': now_utc().isoformat(), 'error': 'no_api_key', 'cities': {}})
        return

    def avg(lst): return round(sum(lst)/len(lst), 3) if lst else None

    # Tankerkoenig sometimes blocks GitHub Actions IPs with 503
    # Use realistic browser headers and add delays
    fuel_session = requests.Session()
    fuel_session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'de-DE,de;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://creativecommons.tankerkoenig.de/',
        'Origin': 'https://creativecommons.tankerkoenig.de',
    })

    def fetch_prices(lat, lon, fuel_type, rad=10, retries=4):
        for attempt in range(retries):
            try:
                r = fuel_session.get(
                    'https://creativecommons.tankerkoenig.de/json/list.php',
                    params={'lat': lat, 'lng': lon, 'rad': rad,
                            'sort': 'price', 'type': fuel_type, 'apikey': api_key},
                    timeout=25
                )
                if r.status_code == 503:
                    wait = 20 + (30 * attempt)
                    print(f'      503 attempt {attempt+1} – sleep {wait}s')
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                d = r.json()
                if not d.get('ok'):
                    raise ValueError(d.get('message',''))
                prices = []
                for s in d.get('stations', []):
                    # type-specific API returns price in 'price' field
                    v = s.get('price') or s.get(fuel_type)
                    if v and 0.5 < float(v) < 5.0:
                        prices.append(round(float(v), 3))
                return sorted(prices)
            except ValueError:
                raise
            except Exception as e:
                if attempt == retries - 1:
                    raise
                time.sleep(10)
        return []

    results = {}
    for city, (lat, lon) in FUEL_CITIES.items():
        city_data = {'count': 0}
        for ft in ['e5', 'e10', 'diesel']:
            try:
                prices = fetch_prices(lat, lon, ft, rad=10)
                city_data[f'{ft}_avg'] = avg(prices)
                city_data[f'{ft}_min'] = prices[0] if prices else None
                city_data['count'] = max(city_data['count'], len(prices))
                time.sleep(2)
            except Exception as e:
                print(f'      {city}/{ft}: {e}')
                time.sleep(5)
        results[city] = city_data
        print(f'    fuel/{city}: E5={city_data.get("e5_avg")}, Diesel={city_data.get("diesel_avg")}')

    nat = {'count': 0}
    for ft in ['e5', 'e10', 'diesel']:
        try:
            prices = fetch_prices(51.163, 10.447, ft, rad=100)
            nat[f'{ft}_avg'] = avg(prices)
            nat['count'] = max(nat['count'], len(prices))
            time.sleep(3)
        except Exception as e:
            print(f'  ! national/{ft}: {e}')
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
    text = re.sub(r'&(?!(amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)', '&amp;', text)
    return text.encode('utf-8')

def fetch_news():
    articles = []
    ATOM = 'http://www.w3.org/2005/Atom'
    for source, url in RSS_FEEDS:
        try:
            r = SESSION.get(url, timeout=12, headers={'Accept':'application/rss+xml,*/*','User-Agent':'Mozilla/5.0'})
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
                title = strip_html(item.findtext('title','') or item.findtext(f'{{{ATOM}}}title',''))
                link_el = item.find(f'{{{ATOM}}}link')
                link = ((link_el.get('href','') if link_el is not None else '') or (item.findtext('link','') or '').strip())
                pubdate = (item.findtext('pubDate','') or item.findtext(f'{{{ATOM}}}published','') or item.findtext(f'{{{ATOM}}}updated',''))
                desc = strip_html(item.findtext('description','') or item.findtext(f'{{{ATOM}}}summary',''))[:280]
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
        'version': '4.4',
    })


# ════════════════════════════════
# 8. ENERGY FUTURES & FORECASTS
# ════════════════════════════════
def fetch_energy_futures():
    """
    Fetches energy price forward curves and forecasts from:
    1. EIA STEO API  — monthly Brent/WTI/EU Gas/Henry Hub incl. 24-month forecast
    2. Yahoo Finance — spot prices (BZ=F, CL=F, TTF=F, NG=F, HO=F)
    3. World Bank    — annual commodity price projections
    4. IMF DataMapper — annual energy price outlook
    5. History JSONL  — saves snapshot for week-over-week comparison
    """
    import os as _os

    eia_key = os.environ.get('EIA_API_KEY', '').strip()
    today_str = now_utc().strftime('%Y-%m-%d')
    current_ym = now_utc().strftime('%Y-%m')  # first forecast month

    # ── 1. EIA STEO ─────────────────────────────────────────────────────────
    EIA_STEO_SERIES = {
        'brent':  'BREPUUS',
        'wti':    'WTIPUUS',
        'eu_gas': 'NGTEIUUS',
        'hh_gas': 'NGHNGUS',
    }
    eia_steo = {'forecast_from': current_ym}

    if eia_key:
        for key, series_id in EIA_STEO_SERIES.items():
            try:
                params = (
                    f'api_key={eia_key}'
                    f'&frequency=monthly'
                    f'&data[0]=value'
                    f'&facets[seriesId][]={series_id}'
                    f'&sort[0][column]=period&sort[0][direction]=asc'
                    f'&offset=0&length=48'
                )
                r = SESSION.get(
                    f'https://api.eia.gov/v2/steo/data/?{params}',
                    timeout=30
                )
                if not r.ok:
                    print(f'  ! eia_steo/{key}: HTTP {r.status_code}')
                    eia_steo[key] = []
                    continue
                rows = r.json().get('response', {}).get('data', [])
                series = []
                for row in rows:
                    period = row.get('period', '')
                    val    = row.get('value')
                    if not period or val is None:
                        continue
                    try:
                        v = round(float(val), 3)
                    except (TypeError, ValueError):
                        continue
                    ptype = 'forecast' if period >= current_ym else 'actual'
                    series.append({'period': period, 'v': v, 'type': ptype})
                eia_steo[key] = series
                n_fc = sum(1 for x in series if x['type'] == 'forecast')
                n_ac = len(series) - n_fc
                print(f'    eia_steo/{key}: {n_ac} actual + {n_fc} forecast pts')
                time.sleep(0.3)
            except Exception as e:
                print(f'  ! eia_steo/{key}: {e}')
                eia_steo[key] = []
    else:
        print('  ! energy_futures: EIA_API_KEY not set — STEO skipped')
        for k in EIA_STEO_SERIES:
            eia_steo[k] = []

    # ── 2. Yahoo Finance spot prices (direct HTTP, no yfinance) ─────────────
    spot = {}
    SPOT_TICKERS = [
        ('brent_usd_bbl',    'BZ%3DF',   'USD/bbl'),
        ('wti_usd_bbl',      'CL%3DF',   'USD/bbl'),
        ('ttf_usd_mmbtu',    'TTF%3DF',  'USD/MMBtu'),
        ('natgas_usd_mmbtu', 'NG%3DF',   'USD/MMBtu'),
        ('heating_usd_gal',  'HO%3DF',   'USD/gal'),
        ('gasoil_usd_t',     'QS%3DF',   'USD/MT'),
    ]
    yh = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
          'Referer': 'https://finance.yahoo.com'}
    for key, enc, unit in SPOT_TICKERS:
        try:
            r = SESSION.get(
                f'https://query1.finance.yahoo.com/v8/finance/chart/{enc}?range=5d&interval=1d',
                timeout=20, headers=yh
            )
            if not r.ok:
                continue
            res = r.json()['chart']['result'][0]
            closes = [c for c in res['indicators']['quote'][0]['close'] if c is not None]
            if closes:
                price = round(closes[-1], 4)
                spot[key] = {'price': price, 'unit': unit, 'date': today_str,
                             'source': 'Yahoo Finance'}
            time.sleep(0.2)
        except Exception as e:
            print(f'  ! yahoo_spot/{key}: {e}')

    # TTF: convert USD/MMBtu → EUR/MWh approx (÷3.41, ×0.92)
    if 'ttf_usd_mmbtu' in spot:
        v = spot['ttf_usd_mmbtu']['price']
        spot['ttf_eur_mwh'] = {
            'price': round(v / 3.41 * 0.92, 2),
            'unit': 'EUR/MWh (approx)',
            'date': today_str,
            'source': 'Yahoo Finance TTF=F converted',
        }
    print(f'    yahoo_spot: {len(spot)} prices')

    # ── 3. World Bank commodity API ─────────────────────────────────────────
    WB_INDICATORS = {'crude_oil': 'POILAPSP', 'eu_gas': 'PNGASEU'}
    worldbank = {}
    cur_year = now_utc().year
    for key, ind in WB_INDICATORS.items():
        try:
            r = SESSION.get(
                f'https://api.worldbank.org/v2/country/WLD/indicator/{ind}'
                f'?format=json&per_page=15&date={cur_year-4}:{cur_year+3}',
                timeout=20
            )
            if not r.ok:
                worldbank[key] = []
                continue
            payload = r.json()
            data_rows = payload[1] if isinstance(payload, list) and len(payload) > 1 else []
            series = []
            for row in (data_rows or []):
                try:
                    yr = int(row.get('date', '0'))
                    val = row.get('value')
                    if val is not None and yr >= 2018:
                        series.append({'year': yr, 'v': round(float(val), 3)})
                except (ValueError, TypeError):
                    pass
            series.sort(key=lambda x: x['year'])
            worldbank[key] = series
            print(f'    worldbank/{key}: {len(series)} years')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! worldbank/{key}: {e}')
            worldbank[key] = []
    worldbank['source'] = 'World Bank Development Data'

    # ── 4. IMF DataMapper ───────────────────────────────────────────────────
    IMF_INDICATORS = {'crude_oil': 'POILAPSP', 'eu_gas': 'PNGASEU'}
    imf = {}
    for key, ind in IMF_INDICATORS.items():
        try:
            r = SESSION.get(
                f'https://www.imf.org/external/datamapper/api/v1/{ind}',
                timeout=20
            )
            if not r.ok:
                imf[key] = []
                continue
            payload = r.json()
            # Find the values dict - structure varies
            vals = {}
            ind_data = payload.get('values', {}).get(ind, {})
            for country_key in ['WORLD', 'WLD', '001']:
                if country_key in ind_data:
                    vals = ind_data[country_key]
                    break
            if not vals:
                # Take first available entry
                for v in ind_data.values():
                    if isinstance(v, dict):
                        vals = v
                        break
            series = []
            for yr_str, val in vals.items():
                try:
                    yr = int(yr_str)
                    if 2018 <= yr <= 2030 and val is not None:
                        series.append({'year': yr, 'v': round(float(val), 3)})
                except (ValueError, TypeError):
                    pass
            series.sort(key=lambda x: x['year'])
            imf[key] = series
            print(f'    imf/{key}: {len(series)} years')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! imf/{key}: {e}')
            imf[key] = []
    imf['source'] = 'IMF World Economic Outlook'

    # ── 5. History snapshot ─────────────────────────────────────────────────
    hist_dir = _os.path.join(OUT, 'history')
    _os.makedirs(hist_dir, exist_ok=True)
    hist_file = _os.path.join(hist_dir, 'futures.jsonl')

    # Save today's snapshot if we have EIA data
    if any(eia_steo.get(k) for k in ['brent', 'wti', 'eu_gas', 'hh_gas']):
        snapshot = {
            'date':       today_str,
            'eia_brent':  eia_steo.get('brent', []),
            'eia_wti':    eia_steo.get('wti', []),
            'eia_eu_gas': eia_steo.get('eu_gas', []),
            'eia_hh_gas': eia_steo.get('hh_gas', []),
        }
        try:
            with open(hist_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(snapshot, ensure_ascii=False) + '\n')
        except Exception as e:
            print(f'  ! futures snapshot: {e}')

    # Load previous week snapshot
    previous_week = None
    target_date = (now_utc() - timedelta(days=7)).strftime('%Y-%m-%d')
    if _os.path.exists(hist_file):
        best, best_delta = None, 999
        try:
            with open(hist_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        rec_d = rec.get('date', '')
                        if rec_d:
                            from datetime import date as _date
                            d1 = datetime.strptime(rec_d, '%Y-%m-%d').date()
                            d2 = datetime.strptime(target_date, '%Y-%m-%d').date()
                            delta = abs((d1 - d2).days)
                            if delta < best_delta:
                                best_delta = delta
                                best = rec
                    except Exception:
                        pass
            if best and best_delta <= 10:
                previous_week = best
                print(f'    futures history: snapshot from {best["date"]} (Δ{best_delta}d)')
        except Exception as e:
            print(f'  ! futures history read: {e}')

    save('energy_futures', {
        'updated':          now_utc().isoformat(),
        'spot':             spot,
        'eia_steo':         eia_steo,
        'worldbank':        worldbank,
        'imf':              imf,
        'history_snapshot': {
            'date':       today_str,
            'eia_brent':  eia_steo.get('brent', []),
            'eia_wti':    eia_steo.get('wti', []),
            'eia_eu_gas': eia_steo.get('eu_gas', []),
            'eia_hh_gas': eia_steo.get('hh_gas', []),
        },
        'previous_week':    previous_week,
    })

if __name__ == '__main__':
    print(f'=== Energy Dashboard Fetch v4.4 – {now_utc().isoformat()} ===\n')
    for label, fn in [
        ('SMARD',            fetch_smard),
        ('Energy-Charts',    fetch_energy_charts),
        ('AGSI+ Gas',        fetch_gas),
        ('Tankerkoenig',     fetch_fuel),
        ('Wetter',           fetch_weather),
        ('Rohstoffe',        fetch_commodities),
        ('Energy Futures',   fetch_energy_futures),
        ('News',             fetch_news),
        ('Meta',             write_meta),
    ]:
        print(f'[{label}]')
        try: fn()
        except Exception as e: print(f'  !! FEHLER: {e}')
        print()
    print(f'=== Fertig: {now_utc().isoformat()} ===')
