"""黑名单: 已知 0 价值粉丝团物提前 drop, 不入 DB 不广播。"""
from __future__ import annotations

import unittest

from hyacinth_sentry.collector import Collector, _ZERO_VALUE_GIFT_IDS


class ZeroValueBlacklistTests(unittest.IsolatedAsyncioTestCase):
    async def test_blacklisted_gfid_is_dropped(self) -> None:
        events = []

        async def on_event(event: dict) -> None:
            events.append(event)

        c = Collector(123, on_event, gift_catalog={}, pandora_catalog={})

        # 陪伴印章 (3410) — 实测 0 价值, 应该被 drop
        await c._dispatch(
            "dgb",
            {"gfid": "3410", "gfcnt": "100", "gfn": "陪伴印章", "uid": "u", "nn": "n"},
            "type@=dgb/gfid@=3410/gfcnt@=100/",
        )
        # 粉丝荧光棒 (824) — variant 也在黑名单
        await c._dispatch(
            "dgb",
            {"gfid": "824", "gfcnt": "30", "gfn": "粉丝荧光棒", "uid": "u", "nn": "n"},
            "type@=dgb/gfid@=824/gfcnt@=30/",
        )
        self.assertEqual(events, [])

    async def test_non_blacklisted_gfid_passes_through(self) -> None:
        events = []

        async def on_event(event: dict) -> None:
            events.append(event)

        # catalog 命中 100 鱼翅的告白飞机, 不在黑名单, 应该入库
        c = Collector(123, on_event, gift_catalog={
            22000: {"name": "告白飞机", "price_yuchi": 100.0, "price_yuwan": 0.0,
                    "intimacy": 1000, "price_type": "YUCHI"},
        }, pandora_catalog={})
        await c._dispatch(
            "dgb",
            {"gfid": "22000", "gfcnt": "1", "uid": "u", "nn": "n"},
            "type@=dgb/gfid@=22000/gfcnt@=1/",
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["gift_name"], "告白飞机")

    def test_blacklist_constant_shape(self) -> None:
        # 确保实测 4 种 (5 个 gfid) 都在
        self.assertIn(3410, _ZERO_VALUE_GIFT_IDS)
        self.assertIn(824, _ZERO_VALUE_GIFT_IDS)
        self.assertIn(1914, _ZERO_VALUE_GIFT_IDS)
        self.assertIn(22899, _ZERO_VALUE_GIFT_IDS)
        self.assertIn(3567, _ZERO_VALUE_GIFT_IDS)


if __name__ == "__main__":
    unittest.main()
