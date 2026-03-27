import asyncio
import base64
import ipaddress
import logging
import re
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone

import aiohttp
from bs4 import BeautifulSoup

PROXY_PATTERN = re.compile(r"\b(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})\b")
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)
USER_AGENT = "proxy-api-aggregator/1.0"
PROTOCOLS = ("http", "socks4", "socks5")
DATASETS = ("all", "live")

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
        "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt",
        "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/http.txt",
        "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt",
        "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
        "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt",
        "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/http_proxies.txt",
        "https://raw.githubusercontent.com/casals-ar/proxy-list/main/http.txt",
        "https://api.openproxylist.xyz/http.txt",
        "https://api.proxyscrape.com/v3/free-proxy-list/get?request=getproxies&protocol=http&timeout=5000&proxy_format=ipport&format=text",
    ],
    "socks4": [
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4_RAW.txt",
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks4&timeout=5000&country=all",
        "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/socks4.txt",
        "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks4.txt",
        "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt",
        "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks4/data.txt",
        "https://raw.githubusercontent.com/casals-ar/proxy-list/main/socks4.txt",
        "https://api.openproxylist.xyz/socks4.txt",
        "https://api.proxyscrape.com/v3/free-proxy-list/get?request=getproxies&protocol=socks4&timeout=5000&proxy_format=ipport&format=text",
    ],
    "socks5": [
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt",
        "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks5&timeout=5000&country=all",
        "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/socks5.txt",
        "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks5.txt",
        "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt",
        "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt",
        "https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/SOCKS5.txt",
        "https://raw.githubusercontent.com/HyperBeats/proxy-list/main/socks5.txt",
        "https://raw.githubusercontent.com/casals-ar/proxy-list/main/socks5.txt",
        "https://api.openproxylist.xyz/socks5.txt",
        "https://api.proxyscrape.com/v3/free-proxy-list/get?request=getproxies&protocol=socks5&timeout=5000&proxy_format=ipport&format=text",
    ],
}

HTML_SOURCES = {
    "http": [
        "https://free-proxy-list.net/",
        "https://www.sslproxies.org/",
    ]
}

HIDEMY_NAME_SOURCE = "https://hidemy.name/en/proxy-list/"
FREEPROXYLISTS_SOURCE = "https://freeproxylists.net/"
ADVANCED_NAME_SOURCE = "https://advanced.name/freeproxy"

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

    def to_dict(self) -> dict[str, str | int]:
        return {
            "ip": self.ip,
            "port": self.port,
            "protocol": self.protocol,
        }

    @classmethod
    def from_dict(cls, proxy: Mapping[str, object]) -> "ProxyEntry":
        return cls(
            ip=str(proxy["ip"]),
            port=int(proxy["port"]),
            protocol=str(proxy["protocol"]),
        )


class ProxyStore:
    def __init__(self) -> None:
        self._data: dict[str, dict[str, set[ProxyEntry]]] = _empty_store()
        self.updated_at: datetime | None = None
        self.validated_at: datetime | None = None
        self._lock = asyncio.Lock()

    async def replace(self, all_proxies: dict[str, set[ProxyEntry]], live_proxies: dict[str, set[ProxyEntry]]) -> None:
        now = datetime.now(timezone.utc)
        async with self._lock:
            self._data = {
                "all": _normalize_proxy_map(all_proxies),
                "live": _normalize_proxy_map(live_proxies),
            }
            self.updated_at = now
            self.validated_at = now

    async def update_live(self, live_proxies: dict[str, set[ProxyEntry]]) -> None:
        async with self._lock:
            self._data["live"] = _normalize_proxy_map(live_proxies)
            self.validated_at = datetime.now(timezone.utc)

    async def get(self, protocol: str, validated: bool = False) -> list[ProxyEntry]:
        dataset = "live" if validated else "all"
        async with self._lock:
            return sorted(self._data[dataset][protocol], key=_sort_key)

    async def get_all_proxy_dicts(self) -> list[dict[str, str | int]]:
        async with self._lock:
            return flatten_proxy_map(self._data["all"])

    async def stats(self) -> dict[str, object]:
        async with self._lock:
            stats: dict[str, object] = {
                "total_scraped": 0,
                "total_live": 0,
                "updated_at": self.updated_at.isoformat() if self.updated_at else None,
                "validated_at": self.validated_at.isoformat() if self.validated_at else None,
            }
            for protocol in PROTOCOLS:
                scraped = len(self._data["all"][protocol])
                live = len(self._data["live"][protocol])
                stats[protocol] = {
                    "scraped": scraped,
                    "live": live,
                }
                stats["total_scraped"] += scraped
                stats["total_live"] += live
            return stats


async def fetch_all_proxies() -> dict[str, set[ProxyEntry]]:
    headers = {"User-Agent": USER_AGENT}
    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT, headers=headers) as session:
        tasks = []
        for protocol, urls in TEXT_SOURCES.items():
            tasks.extend(_fetch_source(session, url, protocol, _fetch_text_source) for url in urls)
        for protocol, urls in HTML_SOURCES.items():
            tasks.extend(_fetch_source(session, url, protocol, _fetch_html_source) for url in urls)
        tasks.extend(
            (
                _fetch_html_site_source(session, HIDEMY_NAME_SOURCE, _fetch_hidemy_name_source),
                _fetch_html_site_source(session, FREEPROXYLISTS_SOURCE, _fetch_freeproxylists_source),
                _fetch_html_site_source(session, ADVANCED_NAME_SOURCE, _fetch_advanced_name_source),
            )
        )
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


async def _fetch_html_site_source(
    session: aiohttp.ClientSession,
    url: str,
    fetcher: Callable[[aiohttp.ClientSession, str], Awaitable[list[ProxyEntry]]],
) -> list[ProxyEntry]:
    try:
        return await fetcher(session, url)
    except Exception as exc:
        logger.warning("Failed to fetch proxies from %s: %s", url, exc)
        return []


async def _fetch_text_source(session: aiohttp.ClientSession, url: str, protocol: str) -> list[ProxyEntry]:
    async with session.get(url) as response:
        response.raise_for_status()
        text = await response.text()
    return list(_parse_raw_text(text, protocol))


async def _fetch_html_source(session: aiohttp.ClientSession, url: str, protocol: str) -> list[ProxyEntry]:
    html = await _fetch_html(session, url)
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


async def _fetch_hidemy_name_source(session: aiohttp.ClientSession, url: str) -> list[ProxyEntry]:
    html = await _fetch_html(session, url)
    soup = BeautifulSoup(html, "lxml")
    entries: set[ProxyEntry] = set()
    rows = soup.select(".table_block table tbody tr") or soup.select("table tbody tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 5:
            continue
        _extend_entries(
            entries,
            ip=cells[0].get_text(strip=True),
            port_value=cells[1].get_text(strip=True),
            protocol_text=cells[4].get_text(" ", strip=True),
            default_protocol="http",
        )
    return list(entries)


async def _fetch_freeproxylists_source(session: aiohttp.ClientSession, url: str) -> list[ProxyEntry]:
    html = await _fetch_html(session, url)
    soup = BeautifulSoup(html, "lxml")
    entries: set[ProxyEntry] = set()
    for row in soup.select("table tr"):
        cells = row.find_all("td")
        if not cells:
            continue
        cell_texts = [cell.get_text(" ", strip=True) for cell in cells]
        row_text = " ".join(cell_texts)
        match = PROXY_PATTERN.search(row_text)
        if match:
            ip, port = match.groups()
            _extend_entries(
                entries,
                ip=ip,
                port_value=port,
                protocol_text=row_text,
                default_protocol="http",
            )
            continue
        if len(cell_texts) >= 2:
            _extend_entries(
                entries,
                ip=cell_texts[0],
                port_value=cell_texts[1],
                protocol_text=row_text,
                default_protocol="http",
            )
    return list(entries)


async def _fetch_advanced_name_source(session: aiohttp.ClientSession, url: str) -> list[ProxyEntry]:
    html = await _fetch_html(session, url)
    soup = BeautifulSoup(html, "lxml")
    entries: set[ProxyEntry] = set()
    rows = soup.select("#table_proxies tbody tr") or soup.select("table tbody tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        ip = _decode_advanced_value(cells[1].get("data-ip")) or cells[1].get_text(strip=True)
        port = _decode_advanced_value(cells[2].get("data-port")) or cells[2].get_text(strip=True)
        _extend_entries(
            entries,
            ip=ip,
            port_value=port,
            protocol_text=cells[3].get_text(" ", strip=True),
            default_protocol="http",
        )
    return list(entries)


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


def flatten_proxy_map(proxies: dict[str, set[ProxyEntry]]) -> list[dict[str, str | int]]:
    items: list[dict[str, str | int]] = []
    for protocol in PROTOCOLS:
        items.extend(entry.to_dict() for entry in sorted(proxies.get(protocol, set()), key=_sort_key))
    return items


def group_proxy_dicts(proxies: list[dict[str, str | int]]) -> dict[str, set[ProxyEntry]]:
    grouped = _empty_proxy_map()
    for proxy in proxies:
        try:
            entry = ProxyEntry.from_dict(proxy)
        except (KeyError, TypeError, ValueError):
            continue
        if entry.protocol in PROTOCOLS and _is_valid_proxy(entry.ip, entry.port):
            grouped[entry.protocol].add(entry)
    return grouped


async def _fetch_html(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.text()


def _extend_entries(
    entries: set[ProxyEntry],
    ip: str,
    port_value: object,
    protocol_text: str,
    default_protocol: str,
) -> None:
    port = _coerce_port(port_value)
    if port is None:
        return
    for protocol in _protocols_from_text(protocol_text, default_protocol):
        if _is_valid_proxy(ip, port):
            entries.add(ProxyEntry(ip=ip, port=port, protocol=protocol))


def _protocols_from_text(text: str, default_protocol: str) -> set[str]:
    lowered = text.lower()
    protocols: set[str] = set()
    if "http" in lowered or "https" in lowered:
        protocols.add("http")
    if "socks4" in lowered:
        protocols.add("socks4")
    if "socks5" in lowered:
        protocols.add("socks5")
    if not protocols and default_protocol in PROTOCOLS:
        protocols.add(default_protocol)
    return protocols


def _decode_advanced_value(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return base64.b64decode(value).decode("utf-8").strip()
    except Exception:
        return None


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


def _empty_store() -> dict[str, dict[str, set[ProxyEntry]]]:
    return {dataset: _empty_proxy_map() for dataset in DATASETS}


def _normalize_proxy_map(proxies: dict[str, set[ProxyEntry]]) -> dict[str, set[ProxyEntry]]:
    return {protocol: set(proxies.get(protocol, set())) for protocol in PROTOCOLS}


def _sort_key(item: ProxyEntry) -> tuple[tuple[int, ...], int]:
    return tuple(int(part) for part in item.ip.split(".")), item.port
