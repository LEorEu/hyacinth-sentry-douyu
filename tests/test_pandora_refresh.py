import unittest

from hyacinth_sentry.collector import Collector


def _dgb_payload(pid: int, name: str) -> dict:
    return {
        "gfid": "0",
        "pid": str(pid),
        "gfcnt": "1",
        "gfn": name,
        "uid": "97720982",
        "nn": "sender",
        "from": "2",
    }


class PandoraRefreshTests(unittest.IsolatedAsyncioTestCase):
    async def test_unknown_pandora_pid_triggers_refresh_and_resolves(self) -> None:
        """主 catalog & pandora 都 miss → 触发 pandora 刷新 → 新 pid 拿到 intimacy。"""
        events: list[dict] = []
        refresh_calls = 0

        async def on_event(event: dict) -> None:
            events.append(event)

        async def pandora_refresher() -> dict[int, dict]:
            nonlocal refresh_calls
            refresh_calls += 1
            return {3971: {"name": "初芒星尘", "intimacy_per_unit": 1},
                    3974: {"name": "闪耀天梯", "intimacy_per_unit": 1000}}

        collector = Collector(
            12740109,
            on_event,
            gift_catalog={},
            pandora_catalog={},
            pandora_catalog_refresher=pandora_refresher,
            catalog_refresh_min_interval=0,
        )

        # 闪耀天梯: intimacy=1000 → effective=100, 远高于阈值, 进 DB
        await collector._dispatch("dgb", _dgb_payload(3974, "闪耀天梯"),
                                  "type@=dgb/gfid@=0/pid@=3974/gfn@=闪耀天梯/")

        self.assertEqual(refresh_calls, 1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["gift_id"], 3974)
        self.assertEqual(events[0]["gift_name"], "闪耀天梯")
        self.assertEqual(events[0]["intimacy"], 1000)
        self.assertEqual(events[0]["effective_yuchi"], 100.0)

    async def test_refresh_skipped_within_min_interval(self) -> None:
        """同一窗口内多笔未知 pid 礼物只刷新一次。"""
        events: list[dict] = []
        refresh_calls = 0

        async def on_event(event: dict) -> None:
            events.append(event)

        async def pandora_refresher() -> dict[int, dict]:
            nonlocal refresh_calls
            refresh_calls += 1
            return {3972: {"name": "追星流光", "intimacy_per_unit": 10}}

        collector = Collector(
            12740109,
            on_event,
            gift_catalog={},
            pandora_catalog={},
            pandora_catalog_refresher=pandora_refresher,
            catalog_refresh_min_interval=300.0,
        )

        # 第一笔 pid=3972 触发刷新, 第二笔 pid=3973 (仍 miss) 不应再次拉
        await collector._dispatch("dgb", _dgb_payload(3972, "追星流光"),
                                  "type@=dgb/gfid@=0/pid@=3972/gfn@=追星流光/")
        await collector._dispatch("dgb", _dgb_payload(3973, "星光之路"),
                                  "type@=dgb/gfid@=0/pid@=3973/gfn@=星光之路/")

        self.assertEqual(refresh_calls, 1)
        # 3972 等效 = 1 鱼翅 < 阈值 6 但 pandora_meta 命中 → 阈值过滤丢弃
        # 3973 完全未收录走安全网 → 保留
        kept_names = [e["gift_name"] for e in events]
        self.assertIn("星光之路", kept_names)
        self.assertNotIn("追星流光", kept_names)

    async def test_refresh_failure_preserves_old_catalog(self) -> None:
        """fetch 抛异常时旧 catalog 不被清空。"""
        events: list[dict] = []

        async def on_event(event: dict) -> None:
            events.append(event)

        async def failing_refresher() -> dict[int, dict]:
            raise RuntimeError("simulated network failure")

        collector = Collector(
            12740109,
            on_event,
            gift_catalog={},
            pandora_catalog={2854: {"name": "清凉椰汁", "intimacy_per_unit": 60}},
            pandora_catalog_refresher=failing_refresher,
            catalog_refresh_min_interval=0,
        )

        # 新 pid 触发刷新, 但 refresher 异常 → 旧 catalog 不变
        await collector._dispatch("dgb", _dgb_payload(9999, "Unknown"),
                                  "type@=dgb/gfid@=0/pid@=9999/gfn@=Unknown/")
        self.assertEqual(collector.pandora_catalog,
                         {2854: {"name": "清凉椰汁", "intimacy_per_unit": 60}})

        # 已收录的 2854 依然正常解析
        await collector._dispatch("dgb", _dgb_payload(2854, "清凉椰汁"),
                                  "type@=dgb/gfid@=0/pid@=2854/gfn@=清凉椰汁/")
        coconut = [e for e in events if e["gift_name"] == "清凉椰汁"]
        self.assertEqual(len(coconut), 1)
        self.assertEqual(coconut[0]["intimacy"], 60)


if __name__ == "__main__":
    unittest.main()
