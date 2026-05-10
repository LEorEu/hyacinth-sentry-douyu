"""End-to-end: spin up the FastAPI app in-process, attach a fake WS sink,
let the real collector connect to room 60937 for ~20s, then assert we got
chat (and possibly gift) events flowing through both DB write + WS broadcast.

Run:  python -m tests.manual_e2e_smoke
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile

os.environ["DOUYU_ROOM_ID"] = "60937"
db_fd, db_path = tempfile.mkstemp(suffix=".db", prefix="douyu_e2e_")
os.close(db_fd)
os.environ["DOUYU_DB"] = db_path

from hyacinth_sentry import server as srv  # noqa: E402

RECEIVED: list[dict] = []


class FakeWS:
    async def send_text(self, msg: str) -> None:
        RECEIVED.append(json.loads(msg))


async def main() -> None:
    fake = FakeWS()
    await srv.hub.add(fake)  # type: ignore[arg-type]

    await srv._startup()
    print(f"running collector for 20s against room {os.environ['DOUYU_ROOM_ID']}...")
    await asyncio.sleep(20)
    await srv._shutdown()

    counts: dict[str, int] = {}
    for e in RECEIVED:
        counts[e["kind"]] = counts.get(e["kind"], 0) + 1
    print(f"\nbroadcast counts: {counts}")
    print(f"first 3 events:")
    for e in RECEIVED[:3]:
        print(" ", e)
    print(f"all gift events:")
    for e in [x for x in RECEIVED if x["kind"] == "gift"]:
        print("  GIFT:", {k: v for k, v in e.items() if k in ("gift_id","gift_name","count","price_yuchi","nickname")})

    # query DB to confirm only non-chat is persisted
    from hyacinth_sentry.db import Store
    s = Store(db_path)
    rows = s.query(room_id=int(os.environ["DOUYU_ROOM_ID"]), limit=500)
    by_kind: dict[str, int] = {}
    for r in rows:
        by_kind[r["kind"]] = by_kind.get(r["kind"], 0) + 1
    print(f"db counts: {by_kind}  (chat MUST be 0)")
    s.close()
    os.unlink(db_path)

    assert "chat" not in by_kind, "chat leaked into DB!"
    print("\nE2E OK")


if __name__ == "__main__":
    asyncio.run(main())
