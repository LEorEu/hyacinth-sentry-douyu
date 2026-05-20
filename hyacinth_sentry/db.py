"""SQLite storage for collected events."""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          INTEGER NOT NULL,
    room_id     INTEGER NOT NULL,
    kind        TEXT    NOT NULL,
    uid         TEXT,
    nickname    TEXT,
    gift_id     INTEGER,
    gift_name   TEXT,
    count       INTEGER DEFAULT 1,
    price_yuchi REAL,
    content     TEXT,
    color       INTEGER,
    raw         TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_room_ts ON events(room_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_kind     ON events(kind);
"""

# Columns added after initial release; applied idempotently to existing DBs.
_MIGRATIONS = [
    ("color",    "INTEGER"),
    ("done_at",  "INTEGER"),  # 高能弹幕"已响应"时间戳 (ms)；NULL = 未处理
    ("intimacy", "INTEGER"),  # 单位亲密度 (pandora/catalog 命中亲密度礼物); 用于 max(price_yuchi, intimacy/10) 等效鱼翅折算
]


class Store:
    def __init__(self, path: str | Path):
        self.path = str(path)
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        existing = {r["name"] for r in self._conn.execute("PRAGMA table_info(events)").fetchall()}
        for col, decl in _MIGRATIONS:
            if col not in existing:
                self._conn.execute(f"ALTER TABLE events ADD COLUMN {col} {decl}")

    def insert(self, event: dict) -> int:
        cur = self._conn.execute(
            """INSERT INTO events
               (ts, room_id, kind, uid, nickname, gift_id, gift_name,
                count, price_yuchi, content, color, raw, intimacy)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                event.get("ts") or int(time.time() * 1000),
                event["room_id"],
                event["kind"],
                event.get("uid"),
                event.get("nickname"),
                event.get("gift_id"),
                event.get("gift_name"),
                event.get("count", 1),
                event.get("price_yuchi"),
                event.get("content"),
                event.get("color"),
                event.get("raw"),
                event.get("intimacy"),
            ),
        )
        return cur.lastrowid

    def query(
        self,
        room_id: int,
        kind: str | None = None,
        uid: str | None = None,
        since_ms: int | None = None,
        until_ms: int | None = None,
        before_id: int | None = None,
        q: str | None = None,
        done_filter: str | None = None,
        hide_zero_value: bool = False,
        limit: int = 100,
    ) -> list[dict]:
        sql = ["SELECT * FROM events WHERE room_id = ?"]
        args: list[Any] = [room_id]
        if kind:
            sql.append("AND kind = ?")
            args.append(kind)
        if hide_zero_value:
            # 屏蔽 price_yuchi 和 intimacy 双 NULL 的礼物 (粉丝团连刷物 / 陪伴印章等)。
            # 让 LIMIT 作用在过滤后, 等于把展示容量留给真正有价值的事件。
            # subscription/superchat 行不命中这个条件 (都有 price_yuchi)。
            sql.append("AND NOT (kind = 'gift' AND price_yuchi IS NULL AND intimacy IS NULL)")
        if uid:
            sql.append("AND uid = ?")
            args.append(uid)
        if since_ms is not None:
            sql.append("AND ts >= ?")
            args.append(since_ms)
        if until_ms is not None:
            sql.append("AND ts <= ?")
            args.append(until_ms)
        if before_id is not None:
            sql.append("AND id < ?")
            args.append(before_id)
        if q:
            # content/nickname/gift_name 模糊匹配; 高能搜索的主用例是 content,
            # 但礼物搜礼物名也合理, 一并扫
            sql.append("AND (content LIKE ? OR nickname LIKE ? OR gift_name LIKE ?)")
            like = f"%{q}%"
            args.extend([like, like, like])
        # 高能弹幕的"已响应/未响应"过滤; 仅 superchat 有 done_at
        if done_filter == "pending":
            sql.append("AND done_at IS NULL")
        elif done_filter == "done":
            sql.append("AND done_at IS NOT NULL")
        sql.append("ORDER BY id DESC LIMIT ?")
        # cap 5000: 一日典型有价值礼物 < 1000, 5k 留出忙日 + 多日跨度余量;
        # 上限存在是防止恶意/笔误 limit=999999 拖垮 SQLite。
        args.append(min(max(1, limit), 5000))
        rows = self._conn.execute(" ".join(sql), args).fetchall()
        return [dict(r) for r in rows]

    def toggle_done(self, event_id: int) -> dict | None:
        """Toggle done_at on a superchat event. Returns updated row dict, or None if not found.

        Only superchat is toggleable; other kinds return None even if the id exists.
        """
        row = self._conn.execute(
            "SELECT id, kind, done_at FROM events WHERE id = ?", (event_id,)
        ).fetchone()
        if not row or row["kind"] != "superchat":
            return None
        new_done = None if row["done_at"] else int(time.time() * 1000)
        self._conn.execute("UPDATE events SET done_at = ? WHERE id = ?", (new_done, event_id))
        return {"id": event_id, "done_at": new_done}

    def export_day(
        self,
        room_id: int,
        since_ms: int,
        until_ms: int,
        kind: str | None = None,
    ) -> list[dict]:
        """Return events for CSV export. Excludes uid (privacy) and raw (size)."""
        sql = [
            "SELECT id, ts, kind, nickname, gift_id, gift_name, count,"
            " price_yuchi, intimacy, content, color, done_at FROM events"
            " WHERE room_id = ? AND ts >= ? AND ts < ?"
        ]
        args: list[Any] = [room_id, since_ms, until_ms]
        if kind:
            sql.append("AND kind = ?")
            args.append(kind)
        sql.append("ORDER BY ts ASC")
        rows = self._conn.execute(" ".join(sql), args).fetchall()
        return [dict(r) for r in rows]

    def stats(self, room_id: int, since_ms: int | None = None) -> dict:
        where = "WHERE room_id = ?"
        args: list[Any] = [room_id]
        if since_ms is not None:
            where += " AND ts >= ?"
            args.append(since_ms)
        total_gift_value = self._conn.execute(
            f"SELECT COALESCE(SUM(price_yuchi * count), 0) FROM events {where} AND kind='gift'",
            args,
        ).fetchone()[0]
        gift_count = self._conn.execute(
            f"SELECT COUNT(*) FROM events {where} AND kind='gift'", args
        ).fetchone()[0]
        sc_count = self._conn.execute(
            f"SELECT COUNT(*) FROM events {where} AND kind='superchat'", args
        ).fetchone()[0]
        top = self._conn.execute(
            f"""SELECT uid, nickname, SUM(price_yuchi * count) AS yuchi, COUNT(*) AS n
                FROM events {where} AND kind='gift' AND uid IS NOT NULL
                GROUP BY uid ORDER BY yuchi DESC LIMIT 10""",
            args,
        ).fetchall()
        return {
            "total_yuchi": total_gift_value,
            "gift_events": gift_count,
            "superchat_events": sc_count,
            "top_senders": [dict(r) for r in top],
        }

    def close(self) -> None:
        self._conn.close()
