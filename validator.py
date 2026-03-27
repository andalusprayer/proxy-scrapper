import asyncio
from collections.abc import Awaitable, Callable

import aiohttp
from aiohttp_socks import ProxyConnector

TEST_URL = "http://httpbin.org/ip"
TIMEOUT = 8
VALIDATION_BATCH_SIZE = 2000


def _build_proxy_url(proxy: dict[str, str | int]) -> str | None:
    proto = str(proxy["protocol"])
    ip = str(proxy["ip"])
    port = int(proxy["port"])
    if proto == "http":
        return f"http://{ip}:{port}"
    if proto == "socks4":
        return f"socks4://{ip}:{port}"
    if proto == "socks5":
        return f"socks5://{ip}:{port}"
    return None


async def check_proxy(session: aiohttp.ClientSession, proxy: dict[str, str | int]) -> bool:
    proxy_url = _build_proxy_url(proxy)
    if proxy_url is None:
        return False

    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    try:
        if str(proxy["protocol"]) == "http":
            async with session.get(TEST_URL, proxy=proxy_url, timeout=timeout, ssl=False) as resp:
                return resp.status == 200

        connector = ProxyConnector.from_url(proxy_url)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as proxied_session:
            async with proxied_session.get(TEST_URL, ssl=False) as resp:
                return resp.status == 200
    except Exception:
        return False


async def validate_proxies(
    proxies: list[dict[str, str | int]],
    concurrency: int = 500,
    on_batch_done: Callable[[list[dict[str, str | int]]], Awaitable[None]] | None = None,
) -> list[dict[str, str | int]]:
    semaphore = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency)
    live_proxies: list[dict[str, str | int]] = []
    async with aiohttp.ClientSession(connector=connector) as session:
        async def run_check(proxy: dict[str, str | int]) -> bool:
            async with semaphore:
                return await check_proxy(session, proxy)

        for index in range(0, len(proxies), VALIDATION_BATCH_SIZE):
            batch = proxies[index:index + VALIDATION_BATCH_SIZE]
            results = await asyncio.gather(*(run_check(proxy) for proxy in batch))
            live_proxies.extend(proxy for proxy, ok in zip(batch, results) if ok)
            if on_batch_done is not None:
                await on_batch_done(list(live_proxies))

    return live_proxies
