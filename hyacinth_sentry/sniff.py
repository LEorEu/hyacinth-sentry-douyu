"""Long-running sniffer that captures EVERY message and writes two logs:

  sniff/<room>_full.log       — all messages, timestamped
  sniff/<room>_interesting.log — only unknown / rare types (i.e. NOT in NOISE)

Usage:
  python -m hyacinth_sentry.sniff 12740109
  # ...let it run, watch the live in browser, note local time when the
  # blue 'SuperChat' card appears, then look at *_interesting.log around
  # that time to find the message type.

Press Ctrl-C to stop.
"""
from __future__ import annotations

import asyncio
import sys
import time
from collections import Counter
from pathlib import Path

from . import protocol as proto

HOST = "danmuproxy.douyu.com"
PORT = 8601

# Types that are too noisy to manually scan; written ONLY to full.log.
NOISE = {
    "chatmsg", "uenter", "oun", "oni", "mrkl", "pingreq",
    "loginres", "actFishing", "defense_tower_session",
    "synexp", "rtss_update", "ranklist", "anchor_rank2505_change",
    "rec_barrage_hot", "configscreen", "blab", "online_noble_list",
    "rss",
}


def _stamp() -> str:
    t = time.time()
    return time.strftime("%H:%M:%S", time.localtime(t)) + f".{int(t * 1000) % 1000:03d}"


async def run(room_id: int, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    full_path = out_dir / f"{room_id}_full.log"
    interesting_path = out_dir / f"{room_id}_interesting.log"
    summary_path = out_dir / f"{room_id}_summary.txt"

    counter: Counter[str] = Counter()

    with open(full_path, "w", encoding="utf-8", buffering=1) as full, \
         open(interesting_path, "w", encoding="utf-8", buffering=1) as inter:
        header = f"# room {room_id}, started {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        full.write(header)
        inter.write(header)
        inter.write(f"# only types NOT in {sorted(NOISE)}\n\n")

        backoff = 1.0
        while True:
            try:
                await _connect_and_capture(room_id, full, inter, counter)
                backoff = 1.0
            except (asyncio.CancelledError, KeyboardInterrupt):
                break
            except Exception as e:
                msg = f"[{_stamp()}] reconnect after error: {e!r}\n"
                full.write(msg); inter.write(msg)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    summary_path.write_text(
        "type counts (descending):\n" +
        "\n".join(f"  {t:30s} {n}" for t, n in counter.most_common()),
        encoding="utf-8",
    )
    print(f"\nsummary written to {summary_path}")


async def _connect_and_capture(room_id, full, inter, counter):
    reader, writer = await asyncio.open_connection(HOST, PORT)
    writer.write(proto.login_req(room_id))
    for gid in (-9999, 1, 2, 3, 4, 5):
        writer.write(proto.join_group(room_id, gid))
    await writer.drain()

    msg = f"[{_stamp()}] connected, joined gids -9999/1..5\n"
    full.write(msg); inter.write(msg)

    async def heartbeat():
        while True:
            await asyncio.sleep(45)
            writer.write(proto.heartbeat())
            await writer.drain()

    hb = asyncio.create_task(heartbeat())
    buf = bytearray()
    try:
        while True:
            chunk = await reader.read(8192)
            if not chunk:
                msg = f"[{_stamp()}] server closed\n"
                full.write(msg); inter.write(msg)
                return
            buf.extend(chunk)
            for _t, body in proto.iter_frames(buf):
                t = body.split("/", 1)[0].replace("type@=", "") if body.startswith("type@=") else "?"
                counter[t] += 1
                line = f"[{_stamp()}] {body}\n"
                full.write(line)
                if t not in NOISE:
                    inter.write(line)
    finally:
        hb.cancel()
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    rid = int(sys.argv[1])
    out_dir = Path(__file__).parent.parent / "sniff"
    print(f"sniffing room {rid}, logs -> {out_dir}")
    print(f"  full:        {rid}_full.log")
    print(f"  interesting: {rid}_interesting.log  <- look here when you see the SC card")
    print("Ctrl-C to stop.\n")
    try:
        asyncio.run(run(rid, out_dir))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
