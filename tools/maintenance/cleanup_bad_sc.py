"""One-shot DB cleanup: remove the mis-classified 'superchat' rows that were
actually plain chatmsg with col≠0 (i.e. paid coloured fan/noble chat, NOT a
real high-energy danmaku).

Heuristic for "bad row":
  kind = 'superchat' AND raw LIKE 'type@=chatmsg/%'

Real high-energy rows have raw starting with 'vrid@=' (followed by btype/...
/type@=comm_chatmsg/...) and a non-null price_yuchi, so they survive.

Backs up the DB before deleting. Run:
  python -m tools.maintenance.cleanup_bad_sc
"""
from __future__ import annotations

import shutil
import sqlite3
import sys
import time
from pathlib import Path

from hyacinth_sentry import PROJECT_DIR

DB = PROJECT_DIR / "events.db"
BAK = DB.with_suffix(f".db.bak-{int(time.time())}")


def main() -> None:
    if not DB.exists():
        sys.exit(f"DB not found: {DB}")

    print(f"DB:     {DB}  ({DB.stat().st_size:,} bytes)")
    print(f"backup: {BAK}")
    shutil.copy2(DB, BAK)
    # Also copy the WAL/SHM in case the live server is running.
    for ext in ("-wal", "-shm"):
        side = DB.with_name(DB.name + ext)
        if side.exists():
            shutil.copy2(side, BAK.with_name(BAK.name + ext))

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    def counts() -> dict:
        return {r["kind"]: r["n"] for r in conn.execute(
            "SELECT kind, COUNT(*) AS n FROM events GROUP BY kind ORDER BY 2 DESC"
        )}

    print("\n--- before ---")
    for k, n in counts().items():
        print(f"  {k:<14} {n}")

    bad = conn.execute(
        "SELECT COUNT(*) FROM events WHERE kind='superchat' AND raw LIKE 'type@=chatmsg/%'"
    ).fetchone()[0]
    legit_v2 = conn.execute(
        "SELECT COUNT(*) FROM events WHERE kind='superchat' AND raw LIKE 'vrid@=%'"
    ).fetchone()[0]
    other_sc = conn.execute(
        "SELECT COUNT(*) FROM events WHERE kind='superchat' AND raw NOT LIKE 'type@=chatmsg/%' AND raw NOT LIKE 'vrid@=%'"
    ).fetchone()[0]
    print(f"\nsuperchat breakdown: bad(col-mislabel)={bad}  legit(comm_chatmsg)={legit_v2}  other={other_sc}")

    if bad == 0:
        print("\nnothing to delete.")
        return

    print(f"\nsamples being deleted (up to 3):")
    for r in conn.execute(
        "SELECT id, ts, nickname, content, color FROM events "
        "WHERE kind='superchat' AND raw LIKE 'type@=chatmsg/%' ORDER BY id LIMIT 3"
    ):
        print(f"  #{r['id']} ts={r['ts']} nick={r['nickname']!r} color={r['color']} text={r['content']!r}")

    cur = conn.execute(
        "DELETE FROM events WHERE kind='superchat' AND raw LIKE 'type@=chatmsg/%'"
    )
    conn.commit()
    print(f"\ndeleted {cur.rowcount} rows.")

    print("\n--- after ---")
    for k, n in counts().items():
        print(f"  {k:<14} {n}")

    # VACUUM to reclaim space (only if no other writer; safe enough since the
    # cleanup is a manual operation usually run while the server is stopped).
    try:
        conn.execute("VACUUM")
        print("\nVACUUM done.")
    except sqlite3.OperationalError as e:
        print(f"\nVACUUM skipped ({e}). Stop the server first if you want to compact.")

    conn.close()


if __name__ == "__main__":
    main()
