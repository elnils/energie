"""
Energy Dashboard – Fetcher v4
Quellen: SMARD, Energy-Charts, AGSI+, Tankerkoenig, Open-Meteo, Yahoo Finance, RSS News
"""
import json, os, re, time, requests, pytz
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

OUT = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(OUT, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 EnergyDashboard/4.0',
    'Accept': 'application/json, */*',
})

def save(name, data):
    path = os.path.join(OUT, f'{name}.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
    print(f'  ✓ {name}.json ({os.path.getsize(path)//1024} KB)')

def now_utc():
    return datetime.now(timezone.utc)

# ════════════════════════════════
# 1. SMARD – Stromerzeugung (15min)
# ════════════════════════════════
SMARD_FILTERS = {
    'wind_onshore': 4067, 'wind_offshore': 1225, 'solar': 4068,
    'biomass': 4066, 'hydro': 1226, 'nuclear': 1224,
    'lignite': 1223, 'hard_coal': 4069, 'natural_gas': 4071,
    'pumped_storage': 4070, 'load': 410,
}

def fetch_smard():
    results = {}
    berlin = pytz.timezone('Europe/Berlin')
    now_b = datetime.now(berlin)
    days_since_mon = now_b.weekday()
    last_mon = now_b - timedelta(days=days_since_mon)
    last_mon_ms = int(last_mon.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    prev_mon_ms = last_mon_ms - 7 * 24 * 3600 * 1000

    for name, fid in SMARD_FILTERS.items():
        series = []
        try:
            url = f'https://www.smard.de/app/chart_data/{fid}/DE/index_quarterhour.json'
            timestamps = SESSION.get(url, timeout=20).json().get('timestamps', [])
            recent = [t for t in timestamps if t >= prev_mon_ms]
            if not recent:
                recent = timestamps[-2:]
            seen = set()
            for bucket in recent[:2]:
                try:
                    url2 = f'https://www.smard.de/app/chart_data/{fid}/DE/{fid}_DE_quarterhour_{bucket}.json'
                    raw = SESSION.get(url2, timeout=20).json().get('series', [])
                    for e in raw:
                        if isinstance(e, list) and len(e) == 2 and e[1] is not None and e[0] not in seen:
                            seen.add(e[0])
                            series.append({'ts': e[0], 'v': round(float(e[1]), 2)})
                    time.sleep(0.05)
                except: pass
            series.sort(key=lambda x: x['ts'])
            print(f'    smard/{name}: {len(series)} Punkte')
        except Exception as e:
            print(f'  ! smard/{name}: {e}')
        results[name] = series

    save('smard', {'updated': now_utc().isoformat(), 'series': results})

# ════════════════════════════════
# 2. ENERGY-CHARTS (Fraunhofer ISE)
# ════════════════════════════════
EC = 'https://api.energy-charts.info'

def ec_get(path, params=None):
    r = SESSION.get(f'{EC}/{path}', params=params, timeout=25)
    r.raise_for_status()
    return r.json()

def fetch_energy_charts():
    results = {}
    end_str = now_utc().strftime('%Y-%m-%d')

    # Strompreise – wichtigste Länder
    for bzn, key in [('DE-LU','price_de'),('AT','price_at'),('FR','price_fr'),('PL','price_pl'),('CH','price_ch')]:
        try:
            d = ec_get('price', {'bzn': bzn, 'start': 'P7D'})
            results[key] = {'unix_seconds': d.get('unix_seconds',[]), 'price': d.get('price',[]), 'unit':'EUR/MWh'}
            print(f'    ec/price {bzn}: {len(d.get("price",[]))} pts')
            time.sleep(0.15)
        except Exception as e:
            print(f'  ! ec/price {bzn}: {e}')
            results[key] = {'unix_seconds':[], 'price':[], 'unit':'EUR/MWh'}

    # Monatliche Preise DE (20 Jahre)
    try:
        d1 = ec_get('price', {'bzn':'DE-LU','start':'2018-10-01','end':end_str,'interval':'month'})
        d2 = ec_get('price', {'bzn':'DE-AT-LU','start':'2005-01-01','end':'2018-09-30','interval':'month'})
        results['price_de_monthly'] = {
            'unix_seconds': d2.get('unix_seconds',[]) + d1.get('unix_seconds',[]),
            'price': d2.get('price',[]) + d1.get('price',[]),
        }
        time.sleep(0.2)
    except Exception as e:
        print(f'  ! ec/monthly: {e}')

    # Öffentliche Erzeugung DE
    try:
        end = now_utc()
        d = ec_get('public_power', {
            'country': 'de',
            'start': (end - timedelta(days=7)).strftime('%Y-%m-%dT%H:%MZ'),
            'end': end.strftime('%Y-%m-%dT%H:%MZ'),
        })
        results['public_power_de'] = d
        time.sleep(0.2)
    except Exception as e:
        print(f'  ! ec/public_power: {e}')

    # EE-Anteil
    try:
        results['ren_share_de'] = ec_get('ren_share_in_public_power', {'country':'de','start':'P30D'})
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/ren_share: {e}')

    # Installierte Leistung DE
    try:
        results['installed_de'] = ec_get('installed_power', {'country':'de','time_step':'yearly'})
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/installed: {e}')

    # Gaspreise TTF
    try:
        results['gas_price'] = ec_get('gas_price', {'start': 'P365D'})
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/gas_price: {e}')

    # CO2 Preis
    try:
        results['co2_price'] = ec_get('co2_price', {'start': 'P365D'})
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/co2_price: {e}')

    # Grenzüberschreitender Handel
    try:
        results['cross_border_de'] = ec_get('cross_border_electricity_trading', {'country':'de','start':'P7D'})
        time.sleep(0.15)
    except Exception as e:
        print(f'  ! ec/cross_border: {e}')

    save('energy_charts', {'updated': now_utc().isoformat(), **results})

# ════════════════════════════════
# 3. AGSI+ Gasspeicher
# ════════════════════════════════
def fetch_gas():
    results = {}
    countries = {'eu':'EU gesamt','de':'Deutschland','at':'Österreich',
                 'fr':'Frankreich','it':'Italien','nl':'Niederlande',
                 'be':'Belgien','pl':'Polen','es':'Spanien'}
    for code, name in countries.items():
        try:
            r = SESSION.get('https://agsi.gie.eu/api', params={'type':code,'size':100}, timeout=25)
            r.raise_for_status()
            raw = r.json().get('data', [])
            cleaned = []
            for entry in raw:
                fill = None
                for field in ['full_is_percentage','trend','gasInStorage']:
                    val = entry.get(field)
                    if val is not None:
                        try: fill = float(val); break
                        except: pass
                date = str(entry.get('gasDayStart','') or entry.get('date','')).strip()[:10]
                def sf(v):
                    try: return float(v or 0)
                    except: return 0.0
                cleaned.append({
                    'date': date, 'fill_pct': fill,
                    'injection': sf(entry.get('injection')),
                    'withdrawal': sf(entry.get('withdrawal')),
                    'working_gas': sf(entry.get('gasInStorage') or entry.get('workingGasVolume')),
                })
            results[code] = {'name': name, 'data': cleaned}
            print(f'    gas/{code}: {len(cleaned)} Einträge')
            time.sleep(0.15)
        except Exception as e:
            print(f'  ! gas/{code}: {e}')
            results[code] = {'name': name, 'data': []}

    save('gas_storage', {'updated': now_utc().isoformat(), **results})

# ════════════════════════════════
# 4. TANKERKOENIG – Kraftstoff
# ════════════════════════════════
FUEL_CITIES = {
    'Berlin':     (52.520, 13.405), 'Hamburg':    (53.550, 10.000),
    'München':    (48.137, 11.575), 'Köln':       (50.938,  6.960),
    'Frankfurt':  (50.110,  8.682), 'Stuttgart':  (48.775,  9.182),
    'Düsseldorf': (51.227,  6.773), 'Leipzig':    (51.340, 12.375),
    'Nürnberg':   (49.452, 11.077), 'Dortmund':   (51.514,  7.465),
    'Bremen':     (53.079,  8.801), 'Hannover':   (52.375,  9.735),
    'Dresden':    (51.050, 13.740), 'Bochum':     (51.481,  7.216),
    'Bonn':       (50.733,  7.100), 'Mannheim':   (49.487,  8.466),
    'Augsburg':   (48.371, 10.898), 'Wiesbaden':  (50.082,  8.240),
    'Bielefeld':  (51.978,  8.532), 'Münster':    (51.962,  7.628),
}

def fetch_fuel():
    api_key = os.environ.get('TANKERKOENIG_API_KEY', '00000000-0000-0000-0000-000000000002')
    results = {}

    def avg(lst):
        return round(sum(lst)/len(lst), 3) if lst else None

    for city, (lat, lon) in FUEL_CITIES.items():
        try:
            r = SESSION.get(
                'https://creativecommons.tankerkoenig.de/json/list.php',
                params={'lat':lat,'lng':lon,'rad':10,'sort':'price','type':'all','apikey':api_key},
                timeout=15
            )
            r.raise_for_status()
            d = r.json()
            if not d.get('ok'):
                results[city] = {'error': d.get('message','api_error')}
                continue
            stations = d.get('stations', [])
            e5  = sorted([s['e5']     for s in stations if isinstance(s.get('e5'),float)     and s['e5']>0.5])
            e10 = sorted([s['e10']    for s in stations if isinstance(s.get('e10'),float)    and s['e10']>0.5])
            die = sorted([s['diesel'] for s in stations if isinstance(s.get('diesel'),float) and s['diesel']>0.5])
            results[city] = {
                'e5_avg': avg(e5), 'e5_min': e5[0] if e5 else None,
                'e10_avg': avg(e10), 'e10_min': e10[0] if e10 else None,
                'diesel_avg': avg(die), 'diesel_min': die[0] if die else None,
                'count': len(stations),
            }
            print(f'    fuel/{city}: E5={results[city]["e5_avg"]}')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! fuel/{city}: {e}')
            results[city] = {}

    # Nationaler Durchschnitt
    try:
        r = SESSION.get(
            'https://creativecommons.tankerkoenig.de/json/list.php',
            params={'lat':51.163,'lng':10.447,'rad':200,'sort':'price','type':'all','apikey':api_key},
            timeout=30
        )
        stations = r.json().get('stations',[])
        e5  = [s['e5']     for s in stations if isinstance(s.get('e5'),float)     and s['e5']>0.5]
        e10 = [s['e10']    for s in stations if isinstance(s.get('e10'),float)    and s['e10']>0.5]
        die = [s['diesel'] for s in stations if isinstance(s.get('diesel'),float) and s['diesel']>0.5]
        results['_national'] = {'e5_avg':avg(e5),'e10_avg':avg(e10),'diesel_avg':avg(die),'count':len(stations)}
    except Exception as e:
        print(f'  ! fuel/national: {e}')

    save('fuel', {'updated': now_utc().isoformat(), 'cities': results})

# ════════════════════════════════
# 5. WETTER (Open-Meteo)
# ════════════════════════════════
WEATHER_CITIES = {
    'Berlin': (52.52, 13.41), 'Hamburg': (53.55, 10.00),
    'München': (48.14, 11.58), 'Frankfurt': (50.11, 8.68),
    'Köln': (50.94, 6.96), 'Stuttgart': (48.78, 9.18),
    'Düsseldorf': (51.22, 6.77), 'Leipzig': (51.34, 12.38),
}

def fetch_weather():
    results = {}
    for city, (lat, lon) in WEATHER_CITIES.items():
        try:
            r = SESSION.get('https://api.open-meteo.com/v1/forecast', params={
                'latitude':lat,'longitude':lon,
                'current':'temperature_2m,wind_speed_10m,wind_direction_10m,cloud_cover,direct_radiation,precipitation,relative_humidity_2m',
                'hourly':'temperature_2m,wind_speed_100m,direct_radiation,cloud_cover,precipitation_probability',
                'daily':'temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,shortwave_radiation_sum,sunrise,sunset',
                'forecast_days':5, 'timezone':'Europe/Berlin'
            }, timeout=15)
            r.raise_for_status()
            results[city] = r.json()
            time.sleep(0.06)
        except Exception as e:
            print(f'  ! weather/{city}: {e}')
    save('weather', {'updated': now_utc().isoformat(), 'cities': results})

# ════════════════════════════════
# 6. ROHSTOFFE (Yahoo Finance)
# ════════════════════════════════
TICKERS = {
    'brent_crude':  ('BZ=F', 'USD/Barrel'),
    'wti_crude':    ('CL=F', 'USD/Barrel'),
    'natgas_henry': ('NG=F', 'USD/MMBtu'),
    'gold':         ('GC=F', 'USD/oz'),
    'coal':         ('MTF=F','USD/t'),
}

def fetch_commodities():
    results = {}
    for name, (ticker, unit) in TICKERS.items():
        try:
            enc = ticker.replace('=','%3D')
            url = f'https://query1.finance.yahoo.com/v8/finance/chart/{enc}?range=2y&interval=1d'
            r = SESSION.get(url, timeout=20, headers={
                'User-Agent':'Mozilla/5.0','Referer':'https://finance.yahoo.com'
            })
            r.raise_for_status()
            res = r.json()['chart']['result'][0]
            ts = res['timestamp']
            closes = res['indicators']['quote'][0]['close']
            series = [{'ts':t,'v':round(c,4)} for t,c in zip(ts,closes) if c is not None]
            results[name] = {'unit':unit,'series':series}
            print(f'    commodity/{name}: {len(series)} pts')
            time.sleep(0.35)
        except Exception as e:
            print(f'  ! commodity/{name}: {e}')
            results[name] = {'unit':unit,'series':[]}
    save('commodities', {'updated': now_utc().isoformat(), **results})

# ════════════════════════════════
# 7. NEWS (RSS)
# ════════════════════════════════
RSS_FEEDS = [
    ('PV Magazine',     'https://www.pv-magazine.de/feed/'),
    ('Energie Zukunft', 'https://www.energiezukunft.eu/feed/'),
    ('Solar Server',    'https://www.solarserver.de/feed/'),
    ('IWR',             'https://www.iwr.de/uploads/tx_vxnewsrss/iwr_rss_feed.xml'),
    ('BNetzA',          'https://www.bundesnetzagentur.de/SiteGlobals/Functions/RSS/DE/RSS-Newsfeed.xml'),
    ('Netzausbau',      'https://www.netzausbau.de/service/rss/de.xml'),
    ('BDEW',            'https://www.bdew.de/service/pressemitteilungen/?format=feed&type=rss'),
    ('Strom-Report',    'https://strom-report.de/feed/'),
]

def strip_html(text):
    return re.sub(r'<[^>]+>', '', text or '').strip()

def fetch_news():
    articles = []
    for source, url in RSS_FEEDS:
        try:
            r = SESSION.get(url, timeout=12, headers={'Accept':'application/rss+xml,*/*'})
            r.raise_for_status()
            root = ET.fromstring(r.content)
            channel = root.find('channel')
            items = channel.findall('item') if channel is not None else []
            for item in items[:10]:
                title   = strip_html(item.findtext('title',''))
                link    = (item.findtext('link','') or '').strip()
                pubdate = item.findtext('pubDate','')
                desc    = strip_html(item.findtext('description',''))[:300]
                if title and link:
                    articles.append({'source':source,'title':title,'link':link,'date':pubdate,'summary':desc})
            print(f'    news/{source}: {min(10,len(items))} Artikel')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! news/{source}: {e}')
    save('news', {'updated': now_utc().isoformat(), 'articles': articles[:100]})

# ════════════════════════════════
# 8. META
# ════════════════════════════════
def write_meta():
    save('meta', {
        'last_fetch': now_utc().isoformat(),
        'next_fetch_approx': (now_utc() + timedelta(minutes=15)).isoformat(),
        'version': '4.0',
    })

# ════════════════════════════════
# MAIN
# ════════════════════════════════
if __name__ == '__main__':
    print(f'=== Energy Dashboard Fetch v4 – {now_utc().isoformat()} ===\n')
    steps = [
        ('SMARD Stromerzeugung',       fetch_smard),
        ('Energy-Charts Fraunhofer',   fetch_energy_charts),
        ('AGSI+ Gasspeicher',          fetch_gas),
        ('Tankerkoenig Kraftstoff',    fetch_fuel),
        ('Open-Meteo Wetter',          fetch_weather),
        ('Yahoo Finance Rohstoffe',    fetch_commodities),
        ('RSS News',                   fetch_news),
        ('Meta',                       write_meta),
    ]
    for label, fn in steps:
        print(f'[{label}]')
        try:
            fn()
        except Exception as e:
            print(f'  !! FEHLER: {e}')
        print()
    print(f'=== Fertig: {now_utc().isoformat()} ===')
