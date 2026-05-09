"""Fetch room gift catalog (id → name, price). Best-effort, never raises."""
from __future__ import annotations

import logging
from typing import Dict

import httpx

log = logging.getLogger(__name__)

# These endpoints return JSON like {"error":0, "data":{"giftList":[{"id":..,"name":..,"priceInfo":{...}}]}}
# v3 is the comprehensive web catalog (~150 gifts/room incl. effects, fan-club, privilege).
# Schema drifts; we only read what we need and fall back gracefully.
_ENDPOINTS = [
    "https://gift.douyucdn.cn/api/gift/v3/web/list?rid={rid}",
    "https://gift.douyucdn.cn/api/gift/v2/web/list?rid={rid}",
]

_PANDORA_ENDPOINT = "https://www.douyu.com/japi/interact/comm/pandora/config?rid={rid}"


async def fetch_gift_catalog(room_id: int) -> Dict[int, dict]:
    """Return {gift_id: {name, price_yuchi, price_yuwan, intimacy, price_type}}.

    priceInfo.priceType 区分货币 (实际观察: YUCHI / YUWAN; 没有 INTIMACY priceType)。
    亲密度礼物在斗鱼协议里其实是 priceType=YUWAN 但 growthInfo.intimacy 较大的礼物
    (如乾坤袋抽出的"抱元守一"). 上层可用 max(price_yuchi, intimacy/10) 做等效鱼翅判断.
    """
    catalog: Dict[int, dict] = {}
    async with httpx.AsyncClient(timeout=10.0) as client:
        for url_tpl in _ENDPOINTS:
            url = url_tpl.format(rid=room_id)
            try:
                r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                log.warning("gift fetch failed for %s: %s", url, e)
                continue
            for item in _extract_gift_list(data):
                gid = _coerce_int(item.get("id") or item.get("gid") or item.get("gfid"))
                if gid is None:
                    continue
                # v3: priceInfo.price 是 centi 单位 (×100 存储), YUCHI 和 YUWAN 都是;
                # v2 'pc' 是 centi-yuchi。
                price_info = item.get("priceInfo") or {}
                ptype = str(price_info.get("priceType") or "YUCHI").upper()
                centi = _coerce_int(
                    price_info.get("price")
                    or item.get("pc")
                    or item.get("price")
                ) or 0
                price_real = centi / 100.0  # 赞=0.1 等小礼物保留小数精度
                growth = item.get("growthInfo") or {}
                intimacy = _coerce_int(growth.get("intimacy")) or 0
                catalog.setdefault(gid, {
                    "name": str(item.get("name") or item.get("gn") or f"礼物#{gid}"),
                    "price_yuchi": price_real if ptype == "YUCHI" else 0.0,
                    "price_yuwan": price_real if ptype == "YUWAN" else 0.0,
                    "intimacy":    intimacy,
                    "price_type":  ptype,
                })
            if catalog:
                break
    log.info("loaded %d gifts for room %d", len(catalog), room_id)
    return catalog


async def fetch_pandora_catalog(room_id: int) -> Dict[int, dict]:
    """Fetch 乾坤袋 (pandora-box) award pool: pid → {name, intimacy_per_unit}.

    乾坤袋抽出的礼物 (抱元守一/千机伞 etc.) 不在主 gift catalog 里, 它们在 dgb 帧里
    带 from=2 + 真实 pid。这个 API 把所有乾坤袋 (按 box gift_id 索引) 的奖品池列出,
    每条 award 含 pid / name / value (单位亲密度) / intimacy (=value*num)。
    我们扁平化出 pid → 单位亲密度 映射, 给 collector 在 catalog miss 时回退使用。
    """
    catalog: Dict[int, dict] = {}
    url = _PANDORA_ENDPOINT.format(rid=room_id)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        log.warning("pandora catalog fetch failed: %s", e)
        return catalog
    boxes = (data or {}).get("data") or {}
    if not isinstance(boxes, dict):
        return catalog
    for box_id, box in boxes.items():
        if not isinstance(box, dict):
            continue
        ratios = box.get("ratio") or {}
        if not isinstance(ratios, dict):
            continue
        for tier_data in ratios.values():
            if not isinstance(tier_data, dict):
                continue
            for award in tier_data.get("award") or []:
                pid = _coerce_int(award.get("pid"))
                if not pid:
                    continue
                value = _coerce_int(award.get("value")) or 0   # 单位亲密度 (=intimacy/num)
                name = str(award.get("name") or f"礼物#{pid}")
                # 同 pid 在不同 ratio 出现多次, 用 value 较大的版本; value 一致则保留首条
                cur = catalog.get(pid)
                if cur is None or value > cur.get("intimacy_per_unit", 0):
                    catalog[pid] = {"name": name, "intimacy_per_unit": value}
    log.info("loaded %d pandora-box award pids for room %d", len(catalog), room_id)
    return catalog


def _extract_gift_list(data) -> list:
    """Walk a few common JSON shapes to find a gift array."""
    if not isinstance(data, dict):
        return []
    d = data.get("data", data)
    for key in ("giftList", "list", "gift_list", "items"):
        v = d.get(key) if isinstance(d, dict) else None
        if isinstance(v, list):
            return v
    if isinstance(d, list):
        return d
    return []


def _coerce_int(v) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except Exception:
            return None
