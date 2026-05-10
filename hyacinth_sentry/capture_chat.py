"""Capture chatmsg events for N seconds to a JSONL file (default 300s = 5min).

Used to tune Phase 2 弹幕过滤参数 against real data. Run alongside the live
server — Douyu allows multiple TCP sessions per room, no conflict.

Output: one JSON object per line: {ts, nn, uid, txt, col}.

  python -m hyacinth_sentry.capture_chat <room_id> [--duration 300] [--out chat_sample.jsonl]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

from . import protocol as proto

HOST = "danmuproxy.douyu.com"
PORT = 8601
GIDS = (-9999, 1, 2, 3, 4, 5)


async def capture(room_id: int, duration: int, out_path: Path) -> None:
    reader, writer = await asyncio.open_connection(HOST, PORT)
    writer.write(proto.login_req(room_id))
    for gid in GIDS:
        writer.write(proto.join_group(room_id, gid))
    await writer.drain()

    async def heartbeat():
        while True:
            await asyncio.sleep(45)
            writer.write(proto.heartbeat())
            await writer.drain()

    hb = asyncio.create_task(heartbeat())
    deadline = time.time() + duration
    started = time.time()
    n = 0
    buf = bytearray()

    with open(out_path, "w", encoding="utf-8", buffering=1) as out:
        try:
            while time.time() < deadline:
                remaining = max(0.1, deadline - time.time())
                try:
                    chunk = await asyncio.wait_for(reader.read(8192), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                if not chunk:
                    print("[capture] server closed", file=sys.stderr)
                    return
                buf.extend(chunk)
                for _t, body in proto.iter_frames(buf):
                    kv = proto.parse_kv(body)
                    if kv.get("type") != "chatmsg":
                        continue
                    out.write(json.dumps({
                        "ts": int(time.time() * 1000),
                        "nn": kv.get("nn"),
                        "uid": kv.get("uid"),
                        "txt": kv.get("txt"),
                        "col": kv.get("col"),
                    }, ensure_ascii=False) + "\n")
                    n += 1
                    if n % 200 == 0:
                        elapsed = time.time() - started
                        print(f"[capture] {n} msgs in {elapsed:.0f}s ({n/max(elapsed,1):.1f}/s)", file=sys.stderr)
        finally:
            hb.cancel()
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    elapsed = time.time() - started
    print(f"\n[capture] done: {n} chatmsg in {elapsed:.0f}s → {out_path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("room_id", type=int)
    ap.add_argument("--duration", type=int, default=300, help="seconds (default 300)")
    ap.add_argument("--out", default="chat_sample.jsonl")
    args = ap.parse_args()
    out = Path(args.out)
    print(f"capturing {args.duration}s of chatmsg from room {args.room_id} → {out}")
    print("(server can keep running, no conflict)")
    try:
        asyncio.run(capture(args.room_id, args.duration, out))
    except KeyboardInterrupt:
        print("\n[capture] interrupted")


if __name__ == "__main__":
    main()
