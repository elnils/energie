"""
News RSS aggregator.

Pulls a curated list of German energy-policy and renewables feeds. Robust
against malformed XML (some feeds slip un-escaped ampersands through).
"""
import re
import time
import xml.etree.ElementTree as ET
from typing import List

from core import http


FEEDS = [
    ('PV Magazine',     'https://www.pv-magazine.de/feed/'),
    ('Energie Zukunft', 'https://www.energiezukunft.eu/feed/'),
    ('Solar Server',    'https://www.solarserver.de/feed/'),
    ('IWR',             'https://www.iwr.de/uploads/tx_vxnewsrss/iwr_rss_feed.xml'),
    ('BNetzA',          'https://www.bundesnetzagentur.de/SiteGlobals/Functions/RSS/DE/RSS-Newsfeed.xml'),
    ('BDEW',            'https://www.bdew.de/service/pressemitteilungen/?format=feed&type=rss'),
    ('Klimareporter',   'https://www.klimareporter.de/feed'),
    ('Heise Energie',   'https://www.heise.de/thema/Energiewende/feed/atom.xml'),
    ('Tecson Ölmarkt',  'https://www.tecson.de/news.xml'),
]

ATOM = 'http://www.w3.org/2005/Atom'


def _strip(text: str) -> str:
    return re.sub(r'<[^>]+>', '', text or '').strip()


def _safe_xml(content: bytes) -> bytes:
    text = content.decode('utf-8', errors='replace')
    text = re.sub(r'&(?!(amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)', '&amp;', text)
    return text.encode('utf-8')


def _parse_feed(source: str, content: bytes) -> List[dict]:
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        root = ET.fromstring(_safe_xml(content))
    channel = root.find('channel')
    items = channel.findall('item') if channel is not None else []
    if not items:
        items = root.findall(f'{{{ATOM}}}entry')
    out = []
    for it in items[:8]:
        title = _strip(it.findtext('title', '') or it.findtext(f'{{{ATOM}}}title', ''))
        link_el = it.find(f'{{{ATOM}}}link')
        link = (link_el.get('href', '') if link_el is not None else '') \
               or (it.findtext('link', '') or '').strip()
        date = (it.findtext('pubDate', '')
                or it.findtext(f'{{{ATOM}}}published', '')
                or it.findtext(f'{{{ATOM}}}updated', ''))
        summary = _strip(it.findtext('description', '')
                          or it.findtext(f'{{{ATOM}}}summary', ''))[:280]
        if title and link:
            out.append({'source': source, 'title': title, 'link': link,
                        'date': date, 'summary': summary})
    return out


def fetch() -> dict:
    s = http.get_session()
    articles: List[dict] = []
    for source, url in FEEDS:
        try:
            r = s.get(url, timeout=12,
                      headers={'Accept': 'application/rss+xml,*/*'})
            r.raise_for_status()
            parsed = _parse_feed(source, r.content)
            articles.extend(parsed)
            print(f'    news/{source}: {len(parsed)}')
            time.sleep(0.3)
        except Exception as e:
            print(f'  ! news/{source}: {e}')

    if not articles:
        raise RuntimeError('News: zero articles fetched')

    return {
        'data': {'articles': articles[:120]},
        'meta': {
            'source': 'Multiple RSS feeds',
        },
    }
