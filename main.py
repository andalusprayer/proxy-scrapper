import asyncio
import logging
from collections.abc import Coroutine
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Literal

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse

from scraper import PROTOCOLS, ProxyEntry, ProxyStore, fetch_all_proxies, flatten_proxy_map, group_proxy_dicts
from validator import validate_proxies

logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler(timezone="UTC")
store = ProxyStore()
job_lock = asyncio.Lock()
startup_tasks: set[asyncio.Task[None]] = set()


async def refresh_proxies() -> None:
    async with job_lock:
        scraped_proxies = await fetch_all_proxies()
        live_proxy_dicts = await validate_proxies(flatten_proxy_map(scraped_proxies))
        await store.replace(scraped_proxies, group_proxy_dicts(live_proxy_dicts))
        stats = await store.stats()
        logger.info(
            "Proxy refresh completed with %s scraped and %s live proxies",
            stats["total_scraped"],
            stats["total_live"],
        )


async def revalidate_proxies() -> None:
    async with job_lock:
        all_proxy_dicts = await store.get_all_proxy_dicts()
        if not all_proxy_dicts:
            logger.info("Skipping proxy revalidation because the scrape cache is empty")
            return

        live_proxy_dicts = await validate_proxies(all_proxy_dicts)
        await store.update_live(group_proxy_dicts(live_proxy_dicts))
        stats = await store.stats()
        logger.info("Proxy revalidation completed with %s live proxies", stats["total_live"])


def _spawn_background_task(coro: Coroutine[object, object, None]) -> None:
    task = asyncio.create_task(coro)
    startup_tasks.add(task)
    task.add_done_callback(_finalize_background_task)


def _finalize_background_task(task: asyncio.Task[None]) -> None:
    startup_tasks.discard(task)
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("Background task failed")


def _to_plain_text(proxies: list[ProxyEntry]) -> str:
    return "\n".join(proxy.value for proxy in proxies)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _spawn_background_task(refresh_proxies())
    scheduler.add_job(refresh_proxies, "interval", minutes=10, id="refresh_proxies", replace_existing=True, max_instances=1)
    scheduler.add_job(
        revalidate_proxies,
        "interval",
        minutes=10,
        id="revalidate_proxies",
        replace_existing=True,
        max_instances=1,
        next_run_time=datetime.now(timezone.utc) + timedelta(minutes=5),
    )
    scheduler.start()
    try:
        yield
    finally:
        for task in tuple(startup_tasks):
            task.cancel()
        if scheduler.running:
            scheduler.shutdown(wait=False)


app = FastAPI(title="Proxy List Aggregator API", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/stats")
async def stats() -> dict[str, object]:
    return await store.stats()


@app.get("/proxies")
async def get_proxies(
    protocol: Literal["http", "socks4", "socks5"] = Query("http"),
    format: Literal["json", "txt"] = Query("json"),
    limit: int = Query(100, ge=1, le=10000),
    validated: bool = Query(False),
):
    proxies = await store.get(protocol, validated=validated)
    limited = proxies[:limit]
    if format == "txt":
        return PlainTextResponse(_to_plain_text(limited))
    return JSONResponse(
        {
            "protocol": protocol,
            "validated": validated,
            "count": len(limited),
            "items": [proxy.value for proxy in limited],
            "updated_at": store.updated_at.isoformat() if store.updated_at else None,
            "validated_at": store.validated_at.isoformat() if store.validated_at else None,
        }
    )


@app.get("/proxies/{protocol}")
async def get_protocol_proxies(
    protocol: str,
    limit: int | None = Query(None, ge=1, le=10000),
    validated: bool = Query(False),
):
    if protocol not in PROTOCOLS:
        raise HTTPException(status_code=404, detail="Protocol not found")
    proxies = await store.get(protocol, validated=validated)
    limited = proxies[:limit] if limit is not None else proxies
    return PlainTextResponse(_to_plain_text(limited))
