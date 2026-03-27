import asyncio
import ipaddress
import logging
import re
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone

import aiohttp
from bs4 import BeautifulSoup

PROXY_PATTERN = re.compile(r"\b(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})\b")
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)
USER_AGENT = "proxy-api-aggregator/1.0"
PROTOCOLS = ("http", "socks4", "socks5")

logger = logging.getLogger(__name__)

TEXT_SOURCES = {
    "http": [
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
        "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
        "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=5000&country=all",
    ],
    "socks4": [
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4_RAW.txt",
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks4&timeout=5000&country=all",
    ],
    "socks5": [
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt",
        "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks5&timeout=5000&country=all",
    ],
}

HTML_SOURCES = {
    "http": [
        "https://free-proxy-list.net/",
        "https://www.sslproxies.org/",
    ]
}

JSON_SOURCES = {
    "http": [
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc",
    ],
    "socks4": [
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc",
    ],
    "socks5": [
        "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc",
    ],
}


@dataclass(frozen=True)
class ProxyEntry:
    ip: str
    port: int
    protocol: str

    @property
    def value(self) -> str:
        return f"{self.ip}:{self.port}"


class ProxyStore:
    def __init__(self) -> None:
        self._data: dict[str, set[ProxyEntry]] = _empty_proxy_map()
        self.updated_at: datetime | None = None
        self._lock = asyncio.Lock()

    async def replace(self, proxies: dict[str, set[ProxyEntry]]) -> None:
        async with self._lock:
            self._data = {
                protocol: set(proxies.get(protocol, set()))
                for protocol in PROTOCOLS
            }
            self.updated_at = datetime.now(timezone.utc)

    async def get(self, protocol: str | None = None) -> dict[str, list[ProxyEntry]] | list[ProxyEntry]:
        async with self._lock:
            if protocol:
                return sorted(self._data[protocol], key=_sort_key)
            return {
                name: sorted(entries, key=_sort_key)
                for name, entries in self._data.items()
            }

    async def stats(self) -> dict[str, int | str | None]:
        async with self._lock:
            http_count = len(self._data["http"])
            socks4_count = len(self._data["socks4"])
            socks5_count = len(self._data["socks5"])
            return {
                "total": http_count + socks4_count + socks5_count,
                "http": http_count,
                "socks4": socks4_count,
                "socks5": socks5_count,
                "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            }


async def fetch_all_proxies() -> dict[str, set[ProxyEntry]]:
    headers = {"User-Agent": USER_AGENT}
    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT, headers=headers) as session:
        tasks = []
        for protocol, urls in TEXT_SOURCES.items():
            tasks.extend(_fetch_source(session, url, protocol, _fetch_text_source) for url in urls)
        for protocol, urls in HTML_SOURCES.items():
            tasks.extend(_fetch_source(session, url, protocol, _fetch_html_source) for url in urls)
        for protocol, urls in JSON_SOURCES.items():
            tasks.extend(_fetch_source(session, url, protocol, _fetch_json_source) for url in urls)
        results = await asyncio.gather(*tasks)

    aggregated = _empty_proxy_map()
    for result in results:
        for entry in result:
            aggregated[entry.protocol].add(entry)
    return aggregated


async def _fetch_source(
    session: aiohttp.ClientSession,
    url: str,
    protocol: str,
    fetcher: Callable[[aiohttp.ClientSession, str, str], Awaitable[list[ProxyEntry]]],
) -> list[ProxyEntry]:
    try:
        return await fetcher(session, url, protocol)
    except Exception as exc:
        logger.warning("Failed to fetch %s proxies from %s: %s", protocol, url, exc)
        return []


async def _fetch_text_source(session: aiohttp.ClientSession, url: str, protocol: str) -> list[ProxyEntry]:
    async with session.get(url) as response:
        response.raise_for_status()
        text = await response.text()
    return list(_parse_raw_text(text, protocol))


async def _fetch_html_source(session: aiohttp.ClientSession, url: str, protocol: str) -> list[ProxyEntry]:
    async with session.get(url) as response:
        response.raise_for_status()
        html = await response.text()

    soup = BeautifulSoup(html, "lxml")
    entries: set[ProxyEntry] = set()
    for row in soup.select("table tbody tr"):
        cells = [cell.get_text(strip=True) for cell in row.find_all("td")]
        if len(cells) < 2:
            continue
        ip = cells[0]
        port = _coerce_port(cells[1])
        if port is not None and _is_valid_proxy(ip, port):
            entries.add(ProxyEntry(ip=ip, port=port, protocol=protocol))

    if entries:
        return list(entries)
    return list(_parse_raw_text(soup.get_text("\n"), protocol))


async def _fetch_json_source(session: aiohttp.ClientSession, url: str, protocol: str) -> list[ProxyEntry]:
    async with session.get(url) as response:
        response.raise_for_status()
        payload = await response.json(content_type=None)

    entries: set[ProxyEntry] = set()
    for item in payload.get("data", []):
        ip = item.get("ip")
        port = _coerce_port(item.get("port"))
        protocols = {str(value).lower() for value in item.get("protocols") or []}
        if not ip or port is None or protocol not in protocols:
            continue
        candidate = f"{ip}:{port}"
        if PROXY_PATTERN.fullmatch(candidate) and _is_valid_proxy(ip, port):
            entries.add(ProxyEntry(ip=ip, port=port, protocol=protocol))
    return list(entries)


def _parse_raw_text(text: str, protocol: str) -> Iterable[ProxyEntry]:
    seen: set[tuple[str, int]] = set()
    for ip, port in PROXY_PATTERN.findall(text):
        port_int = int(port)
        if not _is_valid_proxy(ip, port_int):
            continue
        key = (ip, port_int)
        if key in seen:
            continue
        seen.add(key)
        yield ProxyEntry(ip=ip, port=port_int, protocol=protocol)


def _is_valid_proxy(ip: str, port: int) -> bool:
    if not 1 <= port <= 65535:
        return False
    try:
        ip_obj = ipaddress.ip_address(ip)
        return isinstance(ip_obj, ipaddress.IPv4Address) and ip_obj.is_global
    except ValueError:
        return False


def _coerce_port(value: object) -> int | None:
    try:
        port = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    if not 1 <= port <= 65535:
        return None
    return port


def _empty_proxy_map() -> dict[str, set[ProxyEntry]]:
    return {protocol: set() for protocol in PROTOCOLS}


def _sort_key(item: ProxyEntry) -> tuple[tuple[int, ...], int]:
    return tuple(int(part) for part in item.ip.split(".")), item.port
