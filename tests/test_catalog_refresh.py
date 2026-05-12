import unittest

from hyacinth_sentry.collector import Collector


class CatalogRefreshTests(unittest.IsolatedAsyncioTestCase):
    async def test_unknown_paid_gift_refreshes_catalog_before_event_is_emitted(self) -> None:
        events = []
        refresh_calls = 0

        async def on_event(event: dict) -> None:
            events.append(event)

        async def refresh_catalog() -> dict[int, dict]:
            nonlocal refresh_calls
            refresh_calls += 1
            return {
                24483: {
                    "name": "Shiny Cart",
                    "price_yuchi": 50.0,
                    "price_yuwan": 0.0,
                    "intimacy": 500,
                    "price_type": "YUCHI",
                }
            }

        collector = Collector(
            12740109,
            on_event,
            gift_catalog={},
            gift_catalog_refresher=refresh_catalog,
            catalog_refresh_min_interval=0,
        )

        await collector._dispatch(
            "dgb",
            {
                "gfid": "24483",
                "gfcnt": "1",
                "gfn": "Shiny Cart",
                "uid": "97720982",
                "nn": "sender",
            },
            "type@=dgb/gfid@=24483/gfcnt@=1/gfn@=Shiny Cart/",
        )

        self.assertEqual(refresh_calls, 1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["gift_id"], 24483)
        self.assertEqual(events[0]["gift_name"], "Shiny Cart")
        self.assertEqual(events[0]["price_yuchi"], 50.0)
        self.assertEqual(events[0]["effective_yuchi"], 50.0)


if __name__ == "__main__":
    unittest.main()
