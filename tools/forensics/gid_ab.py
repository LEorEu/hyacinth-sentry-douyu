"""A/B/C test: which gid yields the most chatmsg in 30s?
A: gid=-9999 (current)
B: gid=1 only
C: gid=-9999 + gid=1 + gid=2 + gid=3 + gid=4 + gid=5
"""
from __future__ import annotations

import asyncio
import sys

from hyacinth_sentry import protocol as proto

ROOM = int(sys.argv[1]) if len(sys.argv) > 1 else 60937
DURATION = 30.0


async def run(label: str, gids: list[int]) -> int:
    reader, writer = await asyncio.open_connection("danmuproxy.douyu.com", 8601)
    writer.write(proto.login_req(ROOM))
    for g in gids:
        writer.write(proto.join_group(ROOM, g))
    await writer.drain()

    chat_count = 0
    buf = bytearray()
    deadline = asyncio.get_event_loop().time() + DURATION
    try:
        while asyncio.get_event_loop().time() < deadline:
            try:
                chunk = await asyncio.wait_for(reader.read(8192), timeout=2)
            except asyncio.TimeoutError:
                continue
            if not chunk:
                break
            buf.extend(chunk)
            for _t, body in proto.iter_frames(buf):
                kv = proto.parse_kv(body)
                if kv.get("type") == "chatmsg":
                    chat_count += 1
    finally:
        writer.close()
        await writer.wait_closed()

    print(f"{label:8s} gids={gids} -> chatmsg in {DURATION:.0f}s: {chat_count}")
    return chat_count


async def main():
    print(f"room={ROOM}, each test {DURATION:.0f}s")
    # Sequential, not parallel — same wall-clock might not be fair, but it
    # avoids the same client opening multiple sockets which Douyu may collapse.
    await run("A", [-9999])
    await run("B", [1])
    await run("C", [-9999, 1, 2, 3, 4, 5])


if __name__ == "__main__":
    asyncio.run(main())
