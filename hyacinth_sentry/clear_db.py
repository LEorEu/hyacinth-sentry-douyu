"""Clear the events table. Backs up the DB first.

Run:
  python -m hyacinth_sentry.clear_db          # interactive: asks Y/n
  python -m hyacinth_sentry.clear_db --yes    # skip confirm (for scripts)

Stop the server before running if you want VACUUM to reclaim disk space —
otherwise the WAL keeps the file size and VACUUM will be skipped silently.
"""
from __future__ import annotations

import shutil
import sqlite3
import sys
import time
from pathlib import Path

from . import PROJECT_DIR

DB = PROJECT_DIR / "events.db"


def _confirm(prompt: str) -> bool:
    try:
        ans = input(prompt).strip().lower()
    except EOFError:
        return False
    return ans in ("y", "yes")


def main() -> None:
    yes = "--yes" in sys.argv[1:] or "-y" in sys.argv[1:]

    if not DB.exists():
        sys.exit(f"DB not found: {DB}")

    bak = DB.with_suffix(f".db.bak-clear-{int(time.time())}")
    print(f"DB:     {DB}  ({DB.stat().st_size:,} bytes)")
    print(f"backup: {bak}")

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    counts = {r["kind"]: r["n"] for r in conn.execute(
        "SELECT kind, COUNT(*) AS n FROM events GROUP BY kind ORDER BY 2 DESC"
    )}
    total = sum(counts.values())
    print("\n--- current ---")
    for k, n in counts.items():
        print(f"  {k:<14} {n}")
    print(f"  {'TOTAL':<14} {total}")

    if total == 0:
        print("\nnothing to clear.")
        conn.close()
        return

    if not yes and not _confirm(f"\n确认清空全部 {total} 条事件? [y/N] "):
        print("aborted.")
        conn.close()
        return

    # Snapshot DB + WAL/SHM (server may still be running).
    shutil.copy2(DB, bak)
    for ext in ("-wal", "-shm"):
        side = DB.with_name(DB.name + ext)
        if side.exists():
            shutil.copy2(side, bak.with_name(bak.name + ext))

    cur = conn.execute("DELETE FROM events")
    conn.commit()
    print(f"\ndeleted {cur.rowcount} rows.")

    # Reset autoincrement so new ids start from 1 again. Best-effort.
    try:
        conn.execute("DELETE FROM sqlite_sequence WHERE name='events'")
        conn.commit()
    except sqlite3.OperationalError:
        pass

    try:
        conn.execute("VACUUM")
        print("VACUUM done.")
    except sqlite3.OperationalError as e:
        print(f"VACUUM skipped ({e}). Stop the server first if you want to compact.")

    conn.close()
    print("\ndone. 备份已存:", bak.name)


if __name__ == "__main__":
    main()
