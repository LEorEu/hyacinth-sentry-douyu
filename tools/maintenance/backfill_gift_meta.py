"""一次性回填 events 表的 price_yuchi / intimacy 元数据。

DB 里历史行的 price_yuchi 和 intimacy 可能是 NULL, 原因:
  - dgb 帧下发的 gfid 是斗鱼"基础礼物"低位 ID (192 赞 / 193 弱鸡 / 3496 玫瑰喷泉),
    跟 v3 catalog 用的高位 ID (20006 等) 不一致, 实时 collector 按 gfid 查 catalog miss
    → 入库时这两个字段都是 NULL。
  - "赞 0.1 鱼翅" 这种 < 6 阈值的礼物在 collector miss 时绕过阈值 (旧行为, 已是历史)。
本脚本按 gift_name 反向查 catalog/pandora, 把两个字段都补上 (只补 NULL, 已有值不覆盖)。

跑法 (项目根目录):
    python -m tools.maintenance.backfill_gift_meta --room 12740109
    python -m tools.maintenance.backfill_gift_meta --room 12740109 --dry-run

来源优先级 (跟实时 collector 保持一致):
    price_yuchi: 主 catalog (按 name 匹配)
    intimacy:    主 catalog → pandora 奖品池

效果示例:
    "赞" 561 行: price_yuchi NULL → 0.1, intimacy NULL → 1
    "玫瑰喷泉" 372 行: price_yuchi 仍 NULL (catalog 没鱼翅价), intimacy NULL → 60
    "告白飞机" 86 行: price_yuchi 仍 100, intimacy NULL → 1000

注意:
  - 按 name 匹配, 因斗鱼礼物的 dgb gfid 和 catalog gfid 经常对不上。
    同名同价是斗鱼平台事实, 不会有 ID 错位的语义风险。
  - 历史 Tab 用 renderEvent 直接渲染, 不走前端阈值过滤, 补完会显示正确单价 (含小数)。
  - 礼物 Tab live 列表仍按 effective_yuchi*count >= 6 过滤, 不会被低价礼物刷屏。
"""
from __future__ import annotations

import argparse
import asyncio
import sqlite3
from pathlib import Path

from hyacinth_sentry import PROJECT_DIR
from hyacinth_sentry.db import Store
from hyacinth_sentry.gifts import fetch_gift_catalog, fetch_pandora_catalog


async def build_name_to_meta(room_id: int) -> dict[str, dict]:
    """返回 {gift_name: {price_yuchi: float|None, intimacy: int|None}}。

    主 catalog 提供 price_yuchi 和 intimacy 两个字段; pandora 仅在 catalog 没命中且有
    intimacy_per_unit 时补 intimacy。同名优先取主 catalog (与实时 collector 一致)。
    """
    catalog = await fetch_gift_catalog(room_id)
    pandora = await fetch_pandora_catalog(room_id)
    table: dict[str, dict] = {}
    for m in catalog.values():
        entry = {
            "price_yuchi": m["price_yuchi"] if m["price_yuchi"] > 0 else None,
            "intimacy":    m["intimacy"]    if m["intimacy"]    > 0 else None,
        }
        # 至少有一个非空才记录, 防止纯 yuwan 礼物覆盖了 pandora 的亲密度信息
        if entry["price_yuchi"] is not None or entry["intimacy"] is not None:
            table[m["name"]] = entry
    for m in pandora.values():
        if m["name"] not in table:
            table[m["name"]] = {"price_yuchi": None, "intimacy": m["intimacy_per_unit"]}
    return table


def backfill(db_path: Path, name_table: dict[str, dict], dry_run: bool) -> dict:
    """返回 {'rows_scanned', 'rows_with_match', 'price_yuchi_filled', 'intimacy_filled'}。"""
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute("""
            SELECT id, gift_name, price_yuchi, intimacy FROM events
            WHERE kind='gift' AND (price_yuchi IS NULL OR intimacy IS NULL)
        """).fetchall()
        stats = {"rows_scanned": len(rows), "rows_with_match": 0,
                 "price_yuchi_filled": 0, "intimacy_filled": 0}
        for row_id, name, cur_price, cur_int in rows:
            meta = name_table.get(name)
            if not meta:
                continue
            new_price = cur_price if cur_price is not None else meta["price_yuchi"]
            new_int   = cur_int   if cur_int   is not None else meta["intimacy"]
            # 只在确实有变化时写入, 避免空更新
            if new_price == cur_price and new_int == cur_int:
                continue
            stats["rows_with_match"] += 1
            if new_price != cur_price: stats["price_yuchi_filled"] += 1
            if new_int   != cur_int:   stats["intimacy_filled"]    += 1
            if not dry_run:
                conn.execute("UPDATE events SET price_yuchi=?, intimacy=? WHERE id=?",
                             (new_price, new_int, row_id))
        if not dry_run:
            conn.commit()
        return stats
    finally:
        conn.close()


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--room", type=int, required=True, help="斗鱼房间号 (拉 catalog 用)")
    ap.add_argument("--db", type=Path, default=PROJECT_DIR / "events.db",
                    help="events.db 路径, 默认项目目录下")
    ap.add_argument("--dry-run", action="store_true", help="只统计不写入")
    args = ap.parse_args()

    print(f"DB: {args.db}")
    print(f"Room: {args.room}")
    print(f"Mode: {'DRY-RUN' if args.dry_run else 'WRITE'}")
    Store(args.db).close()  # 触发 schema migration (幂等)
    print("Fetching catalog + pandora ...")
    name_table = await build_name_to_meta(args.room)
    print(f"Loaded {len(name_table)} gift-name → meta entries")

    stats = backfill(args.db, name_table, args.dry_run)
    print(f"Rows scanned:        {stats['rows_scanned']}")
    print(f"Rows with name hit:  {stats['rows_with_match']}")
    print(f"price_yuchi filled:  {stats['price_yuchi_filled']}")
    print(f"intimacy filled:     {stats['intimacy_filled']}")
    if args.dry_run and stats["rows_with_match"]:
        print("(--dry-run, no writes performed; remove --dry-run to commit)")


if __name__ == "__main__":
    asyncio.run(main())
