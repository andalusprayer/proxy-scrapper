import logging
from contextlib import asynccontextmanager
from typing import Literal

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse

from scraper import PROTOCOLS, ProxyEntry, ProxyStore, fetch_all_proxies

logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler(timezone="UTC")
store = ProxyStore()


async def refresh_proxies() -> None:
    proxies = await fetch_all_proxies()
    await store.replace(proxies)
    stats = await store.stats()
    logger.info("Proxy refresh completed with %s total proxies", stats["total"])


def _to_plain_text(proxies: list[ProxyEntry]) -> str:
    return "\n".join(proxy.value for proxy in proxies)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await refresh_proxies()
    except Exception:
        logger.exception("Initial proxy refresh failed; starting with an empty cache")
    scheduler.add_job(refresh_proxies, "interval", minutes=60, id="refresh_proxies", replace_existing=True)
    scheduler.start()
    try:
        yield
    finally:
        if scheduler.running:
            scheduler.shutdown(wait=False)


app = FastAPI(title="Proxy List Aggregator API", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/stats")
async def stats() -> dict[str, int | str | None]:
    return await store.stats()


@app.get("/proxies")
async def get_proxies(
    protocol: Literal["http", "socks4", "socks5"] = Query("http"),
    format: Literal["json", "txt"] = Query("json"),
    limit: int = Query(100, ge=1, le=10000),
):
    proxies = await store.get(protocol)
    limited = proxies[:limit]
    if format == "txt":
        return PlainTextResponse(_to_plain_text(limited))
    return JSONResponse(
        {
            "protocol": protocol,
            "count": len(limited),
            "items": [proxy.value for proxy in limited],
            "updated_at": store.updated_at.isoformat() if store.updated_at else None,
        }
    )


@app.get("/proxies/{protocol}")
async def get_protocol_proxies(protocol: str):
    if protocol not in PROTOCOLS:
        raise HTTPException(status_code=404, detail="Protocol not found")
    proxies = await store.get(protocol)
    return PlainTextResponse(_to_plain_text(proxies))
