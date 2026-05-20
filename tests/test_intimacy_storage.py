"""P4: intimacy 列入库 + 查询契约。"""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from hyacinth_sentry.db import Store


class IntimacyStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "test.db"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_intimacy_round_trip(self) -> None:
        s = Store(self.db_path)
        row_id = s.insert({
            "ts": 1700000000000, "room_id": 1, "kind": "gift",
            "uid": "u1", "nickname": "n", "gift_id": 3496, "gift_name": "玫瑰喷泉",
            "count": 1, "price_yuchi": None, "intimacy": 60,
        })
        rows = s.query(room_id=1, kind="gift")
        s.close()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], row_id)
        self.assertEqual(rows[0]["intimacy"], 60)
        self.assertIsNone(rows[0]["price_yuchi"])

    def test_migration_adds_intimacy_column_to_existing_db(self) -> None:
        # 模拟旧 DB: 用原始 SQL 建一个不含 intimacy 列的 events 表
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("""
            CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER, room_id INTEGER, kind TEXT,
                uid TEXT, nickname TEXT, gift_id INTEGER, gift_name TEXT,
                count INTEGER, price_yuchi REAL, content TEXT, raw TEXT
            )
        """)
        conn.execute("INSERT INTO events (ts, room_id, kind, gift_name, count) "
                     "VALUES (1, 1, 'gift', '老礼物', 1)")
        conn.commit()
        conn.close()

        # 打开 Store → 触发 migration
        s = Store(self.db_path)
        cols = {r["name"] for r in s._conn.execute("PRAGMA table_info(events)").fetchall()}
        self.assertIn("intimacy", cols)
        # 老行 intimacy 应为 NULL
        rows = s.query(room_id=1, kind="gift")
        self.assertEqual(rows[0]["gift_name"], "老礼物")
        self.assertIsNone(rows[0]["intimacy"])
        s.close()

    def test_intimacy_omitted_defaults_to_null(self) -> None:
        s = Store(self.db_path)
        s.insert({
            "ts": 1, "room_id": 1, "kind": "gift", "gift_name": "yuchi-only",
            "price_yuchi": 100,
        })
        rows = s.query(room_id=1, kind="gift")
        s.close()
        self.assertIsNone(rows[0]["intimacy"])
        self.assertEqual(rows[0]["price_yuchi"], 100)

    def test_hide_zero_value_filters_double_null_gifts(self) -> None:
        s = Store(self.db_path)
        # 一条 0 价值礼物 (陪伴印章): 两个字段都 NULL
        s.insert({"ts": 1, "room_id": 1, "kind": "gift", "gift_name": "陪伴印章",
                  "price_yuchi": None, "intimacy": None})
        # 一条亲密度礼物: price_yuchi NULL 但 intimacy 有值
        s.insert({"ts": 2, "room_id": 1, "kind": "gift", "gift_name": "玫瑰喷泉",
                  "price_yuchi": None, "intimacy": 60})
        # 一条鱼翅礼物: price_yuchi 有值, intimacy NULL
        s.insert({"ts": 3, "room_id": 1, "kind": "gift", "gift_name": "告白飞机",
                  "price_yuchi": 100, "intimacy": None})

        # 默认不过滤: 3 条都返回
        all_rows = s.query(room_id=1, kind="gift")
        self.assertEqual({r["gift_name"] for r in all_rows},
                         {"陪伴印章", "玫瑰喷泉", "告白飞机"})

        # hide_zero_value=True: 只丢"陪伴印章" (双 NULL), 其余保留
        filtered = s.query(room_id=1, kind="gift", hide_zero_value=True)
        self.assertEqual({r["gift_name"] for r in filtered},
                         {"玫瑰喷泉", "告白飞机"})
        s.close()

    def test_hide_zero_value_keeps_subscription_with_null_yuchi(self) -> None:
        # 防御性测试: subscription 行 (例如某些贵族开通) price_yuchi 可能 NULL
        # 但 kind!='gift', 所以 hide_zero_value 不应该误伤
        s = Store(self.db_path)
        s.insert({"ts": 1, "room_id": 1, "kind": "subscription", "gift_name": "开通骑士",
                  "price_yuchi": None, "intimacy": None})
        rows = s.query(room_id=1, hide_zero_value=True)
        s.close()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["gift_name"], "开通骑士")


if __name__ == "__main__":
    unittest.main()
