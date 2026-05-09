"""FastAPI entry: hosts the static page, history REST, and live WebSocket."""
from __future__ import annotations

import asyncio
import contextlib
import csv
import io
import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .collector import Collector
from .db import Store
from .gifts import fetch_gift_catalog, fetch_pandora_catalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

ROOM_ID = int(os.environ.get("DOUYU_ROOM_ID", "0"))
DB_PATH = os.environ.get("DOUYU_DB", str(Path(__file__).parent / "events.db"))
ADMIN_KEY = os.environ.get("DOUYU_ADMIN_KEY", "")  # empty = single-user mode (no auth)
STATIC_DIR = Path(__file__).parent / "static"

BETARD_URL = "https://www.douyu.com/betard/{rid}"
BETARD_INTERVAL = 8.0  # seconds; just for "随便扫一眼" — no need to spam

app = FastAPI(title="Hyacinth Sentry (风信子哨兵)")
store = Store(DB_PATH)


class Hub:
    """Tiny pub/sub: collector pushes events, all WS clients receive."""

    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def add(self, ws: WebSocket) -> None:
        async with self._lock:
            self.clients.add(ws)

    async def remove(self, ws: WebSocket) -> None:
        async with self._lock:
            self.clients.discard(ws)

    async def broadcast(self, payload: dict) -> None:
        msg = json.dumps(payload, ensure_ascii=False)
        dead: list[WebSocket] = []
        async with self._lock:
            targets = list(self.clients)
        for ws in targets:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    self.clients.discard(ws)


hub = Hub()
collector: Collector | None = None
_betard_task: asyncio.Task | None = None


# ---- helpers --------------------------------------------------------------


def _to_int(v) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except Exception:
            return None


def _4am_window_ms(date_str: str | None = None) -> tuple[int, int]:
    """Return (since_ms, until_ms) for a 04:00→04:00 local-time day window.

    date_str (YYYY-MM-DD) → that day's 04:00 to next day 04:00.
    None → current "今日"; if it's 02:30 we still belong to yesterday's window.
    """
    if date_str:
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(400, "bad date format, expected YYYY-MM-DD")
        start = d.replace(hour=4, minute=0, second=0, microsecond=0)
    else:
        now = datetime.now()
        anchor = now.replace(hour=4, minute=0, second=0, microsecond=0)
        if now < anchor:
            anchor -= timedelta(days=1)
        start = anchor
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _require_admin(
    x_admin_key: str | None = Header(None),
    key: str | None = Query(None),
) -> bool:
    """Admin gate. Empty ADMIN_KEY → solo-user mode, no auth required."""
    if not ADMIN_KEY:
        return True
    if (x_admin_key or key) != ADMIN_KEY:
        raise HTTPException(403, "admin key invalid")
    return True


# ---- collector glue --------------------------------------------------------


async def _on_event(event: dict) -> None:
    payload = dict(event)
    payload.pop("raw", None)  # don't ship raw KV blob to the browser
    if event["kind"] in ("chat", "hot_barrage", "vip_info"):
        # ephemeral: broadcast only, never stored
        await hub.broadcast(payload)
        return
    event_id = store.insert(event)
    payload["id"] = event_id
    await hub.broadcast(payload)


async def _betard_loop() -> None:
    """Poll Douyu's public room metadata endpoint, broadcast room_info events.

    Best-effort, ignored if endpoint changes shape; only viewer count drives UI.
    """
    if ROOM_ID <= 0:
        return
    url = BETARD_URL.format(rid=ROOM_ID)
    last: dict | None = None
    headers = {"User-Agent": "Mozilla/5.0"}
    async with httpx.AsyncClient(timeout=8.0, headers=headers) as client:
        while True:
            try:
                r = await client.get(url)
                r.raise_for_status()
                data = r.json()
                # betard wraps payload variously: {error, data:{room:{...}}} or
                # {room:{...}} or direct fields. Walk a few likely shapes.
                room = data.get("room") or (data.get("data") or {}).get("room") or data.get("data") or data
                if not isinstance(room, dict):
                    room = {}
                # 在线热度(hn/online/hot)用户已确认无意义,贵宾数走 oni 推送; 这里只保留开播状态
                payload = {
                    "kind": "room_info",
                    "ts": int(time.time() * 1000),
                    "show_status": _to_int(room.get("show_status")),
                    "room_name": room.get("room_name") or None,
                    "owner_name": room.get("owner_name") or None,
                }
                if payload != last:
                    await hub.broadcast(payload)
                    last = payload
            except Exception as e:
                log.debug("betard fetch failed: %s", e)
            await asyncio.sleep(BETARD_INTERVAL)


# ---- lifecycle -------------------------------------------------------------


@app.on_event("startup")
async def _startup() -> None:
    global collector, _betard_task
    if ROOM_ID <= 0:
        log.error("DOUYU_ROOM_ID is not set; collector will NOT start")
        return
    catalog = await fetch_gift_catalog(ROOM_ID)
    pandora = await fetch_pandora_catalog(ROOM_ID)
    collector = Collector(ROOM_ID, _on_event, gift_catalog=catalog, pandora_catalog=pandora)
    collector.start()
    _betard_task = asyncio.create_task(_betard_loop(), name=f"betard-{ROOM_ID}")
    log.info("collector started for room %d, db=%s", ROOM_ID, DB_PATH)
    if ADMIN_KEY:
        log.info("admin mode enabled (DOUYU_ADMIN_KEY set)")
    else:
        log.info("admin mode disabled — solo-user (set DOUYU_ADMIN_KEY to gate write actions)")


@app.on_event("shutdown")
async def _shutdown() -> None:
    if _betard_task:
        _betard_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await _betard_task
    if collector:
        await collector.stop()
    store.close()


# ---- REST ------------------------------------------------------------------


@app.get("/api/config")
async def api_config():
    return {"room_id": ROOM_ID}


@app.get("/api/me")
async def api_me(
    x_admin_key: str | None = Header(None),
    key: str | None = Query(None),
):
    if not ADMIN_KEY:
        return {"is_admin": True, "admin_enabled": False}
    is_admin = (x_admin_key == ADMIN_KEY) or (key == ADMIN_KEY)
    return {"is_admin": is_admin, "admin_enabled": True}


@app.get("/api/history")
async def api_history(
    kind: str | None = Query(None, pattern="^(gift|superchat|subscription)$"),
    uid: str | None = None,
    since_ms: int | None = None,
    until_ms: int | None = None,
    before_id: int | None = None,
    limit: int = 100,
):
    if ROOM_ID <= 0:
        raise HTTPException(400, "room not configured")
    return store.query(
        room_id=ROOM_ID,
        kind=kind,
        uid=uid,
        since_ms=since_ms,
        until_ms=until_ms,
        before_id=before_id,
        limit=limit,
    )


@app.get("/api/stats")
async def api_stats(since_ms: int | None = None):
    if ROOM_ID <= 0:
        raise HTTPException(400, "room not configured")
    if since_ms is None:
        since_ms, _ = _4am_window_ms()
    return store.stats(room_id=ROOM_ID, since_ms=since_ms)


@app.post("/api/event/{event_id}/done")
async def api_event_done(event_id: int, _: bool = Depends(_require_admin)):
    res = store.toggle_done(event_id)
    if res is None:
        raise HTTPException(404, "event not found or not toggleable")
    await hub.broadcast({"kind": "done_changed", **res})
    return res


@app.get("/api/export.csv")
async def api_export_csv(
    date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    kind: str | None = Query(None, pattern="^(gift|superchat|subscription)$"),
    _: bool = Depends(_require_admin),
):
    if ROOM_ID <= 0:
        raise HTTPException(400, "room not configured")
    since_ms, until_ms = _4am_window_ms(date)
    rows = store.export_day(ROOM_ID, since_ms, until_ms, kind=kind)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow([
        "id", "ts", "iso_time", "kind", "nickname",
        "gift_id", "gift_name", "count", "price_yuchi", "content", "color", "done_at",
    ])
    for r in rows:
        iso = datetime.fromtimestamp(r["ts"] / 1000).isoformat(sep=" ", timespec="seconds")
        w.writerow([
            r["id"], r["ts"], iso, r["kind"], r["nickname"],
            r["gift_id"], r["gift_name"], r["count"], r["price_yuchi"],
            r["content"], r["color"], r.get("done_at"),
        ])
    label = date or datetime.now().strftime("%Y-%m-%d")
    body = out.getvalue().encode("utf-8-sig")  # BOM so Excel opens UTF-8 cleanly
    return StreamingResponse(
        iter([body]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="douyu_{ROOM_ID}_{label}.csv"'},
    )


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await ws.accept()
    await hub.add(ws)
    try:
        while True:
            # We don't consume client messages; this just keeps the socket open.
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await hub.remove(ws)


@app.get("/")
async def root():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
