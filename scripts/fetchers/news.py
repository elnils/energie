"""
News aggregator — RSS feeds for German + international energy coverage.

Strategy: Two layers.
  1. Direct RSS feeds where the publisher offers them (PV Magazine,
     Energie Zukunft, Heise Energie, etc.).
  2. Google News RSS as a stable proxy for sites that:
     - Don't offer RSS (FT, WSJ, NYT, Bloomberg behind paywalls)
     - Have unstable RSS URLs (BNetzA, BDEW, IWR moved repeatedly)
     - Need topical filtering (Reuters by keyword)

Google News RSS URL pattern:
    https://news.google.com/rss/search?q=<query>&hl=<lang>&gl=<country>&ceid=<region>
"""
import re
import time
from typing import Dict, List, Tuple
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus

from core import http


DIRECT_FEEDS: List[Tuple[str, str, str]] = [
    ('PV Magazine',     'DE Energie',  'https://www.pv-magazine.de/feed/'),
    ('Energie Zukunft', 'DE Energie',  'https://www.energiezukunft.eu/feed/'),
    ('Solar Server',    'DE Energie',  'https://www.solarserver.de/feed/'),
    ('Klimareporter',   'DE Energie',  'https://www.klimareporter.de/feed'),
    ('Heise Energie',   'DE Tech',     'https://www.heise.de/thema/Energiewende/feed/atom.xml'),
    ('SMARD Ticker',    'DE Behörden', 'https://www.smard.de/home/rss'),
]

GOOGLE_NEWS_FEEDS: List[Tuple[str, str, str, str]] = [
    ('BNetzA',          'DE Behörden', 'site:bundesnetzagentur.de Energie OR Strom OR Gas', 'de'),
    ('BDEW',            'DE Verbände', 'site:bdew.de Pressemitteilung', 'de'),
    ('IWR',             'DE Energie',  'site:iwr.de Energie', 'de'),
    ('Energiekrise DE', 'DE Themen',   'Energiekrise OR Gaspreise OR Strompreise Deutschland', 'de'),
    ('Energiepolitik',  'DE Themen',   'Energiepolitik OR Energiewende Bundesregierung', 'de'),
    ('Financial Times', 'EN Wirtschaft', 'site:ft.com energy OR oil OR gas OR LNG', 'en'),
    ('WSJ Energy',      'EN Wirtschaft', 'site:wsj.com energy', 'en'),
    ('NYT Energy',      'EN Wirtschaft', 'site:nytimes.com energy OR oil OR gas', 'en'),
    ('Reuters Energy',  'EN Wirtschaft', 'site:reuters.com energy OR oil OR gas', 'en'),
    ('Bloomberg Energy','EN Wirtschaft', 'site:bloomberg.com energy OR oil OR gas', 'en'),
    ('EurActiv Energy', 'EU Politik',  'site:euractiv.com energy', 'en'),
    ('Politico EU',     'EU Politik',  'site:politico.eu energy', 'en'),
    ('Le Monde Energie','EU Politik',  'site:lemonde.fr énergie OR pétrole OR gaz', 'fr'),
    ('ICIS',            'Spezial',     'site:icis.com gas OR LNG OR power', 'en'),
    ('S&P Platts',      'Spezial',     '"S&P Global" OR Platts gas OR LNG OR power', 'en'),
    ('Hormus Crisis',   'Krise',       'Strait of Hormuz OR Iran oil tanker', 'en'),
]


def _gnews_url(query: str, lang: str = 'de') -> str:
    locales = {
        'de': ('de', 'DE', 'DE:de'),
        'en': ('en', 'US', 'US:en'),
        'fr': ('fr', 'FR', 'FR:fr'),
    }
    hl, gl, ceid = locales.get(lang, locales['en'])
    return (f'https://news.google.com/rss/search?'
            f'q={quote_plus(query)}&hl={hl}&gl={gl}&ceid={ceid}')


def _strip_html(text: str) -> str:
    if not text:
        return ''
    return re.sub(r'<[^>]+>', '', text).strip()


def _clean_xml_bytes(content: bytes) -> bytes:
    text = content.decode('utf-8', errors='replace')
    text = re.sub(r'&(?!(amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)', '&amp;', text)
    return text.encode('utf-8')


def _parse_feed(content: bytes) -> List[dict]:
    ATOM = 'http://www.w3.org/2005/Atom'
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        root = ET.fromstring(_clean_xml_bytes(content))

    items = []
    channel = root.find('channel')
    rss_items = channel.findall('item') if channel is not None else []
    atom_items = root.findall(f'{{{ATOM}}}entry')

    for item in rss_items[:10]:
        title = _strip_html(item.findtext('title', ''))
        link = (item.findtext('link', '') or '').strip()
        date = item.findtext('pubDate', '') or ''
        desc = _strip_html(item.findtext('description', ''))[:280]
        if title and link:
            items.append({'title': title, 'link': link, 'date': date, 'summary': desc})

    for item in atom_items[:10]:
        title = _strip_html(item.findtext(f'{{{ATOM}}}title', ''))
        link_el = item.find(f'{{{ATOM}}}link')
        link = (link_el.get('href', '') if link_el is not None else '').strip()
        date = item.findtext(f'{{{ATOM}}}published', '') or item.findtext(f'{{{ATOM}}}updated', '')
        desc = _strip_html(item.findtext(f'{{{ATOM}}}summary', ''))[:280]
        if title and link:
            items.append({'title': title, 'link': link, 'date': date, 'summary': desc})

    return items


def _fetch_one(url: str, timeout: int = 12) -> List[dict]:
    s = http.get_session()
    headers = {
        'Accept': 'application/rss+xml, application/atom+xml, application/xml, text/xml, */*',
        'User-Agent': 'Mozilla/5.0 (compatible; EnergyDashboard/5.0)',
    }
    r = s.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return _parse_feed(r.content)


def fetch() -> dict:
    articles: List[dict] = []
    sources_status: List[dict] = []

    for label, category, url in DIRECT_FEEDS:
        try:
            items = _fetch_one(url)
            for it in items:
                articles.append({'source': label, 'category': category, **it})
            sources_status.append({'label': label, 'category': category,
                                   'count': len(items), 'status': 'ok'})
            print(f'    news/{label}: {len(items)}')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! news/{label}: {e}')
            sources_status.append({'label': label, 'category': category,
                                   'count': 0, 'status': 'error',
                                   'error': str(e)[:200]})

    for label, category, query, lang in GOOGLE_NEWS_FEEDS:
        url = _gnews_url(query, lang)
        try:
            items = _fetch_one(url)
            for it in items:
                articles.append({'source': label, 'category': category, **it})
            sources_status.append({'label': label, 'category': category,
                                   'count': len(items), 'status': 'ok'})
            print(f'    gnews/{label}: {len(items)}')
            time.sleep(0.4)
        except Exception as e:
            print(f'  ! gnews/{label}: {e}')
            sources_status.append({'label': label, 'category': category,
                                   'count': 0, 'status': 'error',
                                   'error': str(e)[:200]})

    seen_links = set()
    deduped = []
    for a in articles:
        key = a['link'].split('?')[0]
        if key in seen_links:
            continue
        seen_links.add(key)
        deduped.append(a)

    deduped.sort(key=lambda a: a.get('date', ''), reverse=True)
    deduped = deduped[:200]

    return {
        'data': {
            'articles': deduped,
            'sources': sources_status,
        },
        'meta': {
            'source': 'aggregated RSS + Google News RSS',
            'feeds_total': len(DIRECT_FEEDS) + len(GOOGLE_NEWS_FEEDS),
            'feeds_ok': sum(1 for s in sources_status if s['status'] == 'ok'),
        },
    }
