"""Shared requests session with retries and a sensible UA."""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import paths

_session: requests.Session = None


def get_session() -> requests.Session:
    """Return a process-wide session with conservative retry on 5xx + 429."""
    global _session
    if _session is None:
        s = requests.Session()
        s.headers.update({
            'User-Agent': paths.USER_AGENT,
            'Accept': 'application/json, application/xml, text/xml, text/html, */*',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.7',
        })
        retry = Retry(
            total=3, backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(['GET', 'HEAD', 'POST']),
            respect_retry_after_header=True,
        )
        s.mount('https://', HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10))
        s.mount('http://', HTTPAdapter(max_retries=retry))
        _session = s
    return _session
