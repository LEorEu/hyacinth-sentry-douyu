"""One-shot probe: connect, login, join, dump every message body for ~25s.
Run with:  python -m tools.forensics.probe 12740109
"""
from __future__ import annotations

import asyncio
import sys
from collections import Counter

from hyacinth_sentry import protocol as proto

HOST = "danmuproxy.douyu.com"
PORT = 8601


async def main(room_id: int, duration: float = 60.0) -> None:
    print(f"connecting {HOST}:{PORT} room={room_id}")
    reader, writer = await asyncio.open_connection(HOST, PORT)
    writer.write(proto.login_req(room_id))
    writer.write(proto.join_group(room_id))
    await writer.drain()
    print("login + joingroup sent, listening...")

    async def hb():
        while True:
            await asyncio.sleep(45)
            writer.write(proto.heartbeat())
            await writer.drain()

    hb_task = asyncio.create_task(hb())
    counter: Counter[str] = Counter()
    samples: dict[str, str] = {}
    buf = bytearray()
    try:
        deadline = asyncio.get_event_loop().time() + duration
        while asyncio.get_event_loop().time() < deadline:
            try:
                chunk = await asyncio.wait_for(reader.read(8192), timeout=duration)
            except asyncio.TimeoutError:
                break
            if not chunk:
                print("server closed")
                break
            buf.extend(chunk)
            for _t, body in proto.iter_frames(buf):
                kv = proto.parse_kv(body)
                t = kv.get("type", "?")
                counter[t] += 1
                # keep one full sample per type for inspection
                samples.setdefault(t, body)
    finally:
        hb_task.cancel()
        writer.close()
        await writer.wait_closed()

    print("\n=== type counts ===")
    for t, n in counter.most_common():
        print(f"  {t:20s} {n}")
    print("\n=== one sample per type (interesting types only) ===")
    for t in ("loginres", "dgb", "ssd", "chatmsg", "spbc", "uenter", "ranklist", "blab", "anbc", "rnewbc"):
        if t in samples:
            body = samples[t]
            print(f"\n[{t}]\n{body[:600]}")


if __name__ == "__main__":
    rid = int(sys.argv[1]) if len(sys.argv) > 1 else 12740109
    asyncio.run(main(rid))
