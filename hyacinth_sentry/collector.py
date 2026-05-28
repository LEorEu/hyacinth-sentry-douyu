"""Async TCP collector for Douyu danmaku server.

Connects to openbarrage.douyu.com:8601, logs in anonymously, joins the room
group, sends heartbeats, and parses dgb / ssd messages into normalized events
which are passed to an on_event callback.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import OrderedDict
from pathlib import Path
from typing import Awaitable, Callable, Dict

from . import PROJECT_DIR, protocol as proto

log = logging.getLogger(__name__)

DANMAKU_HOST = "danmuproxy.douyu.com"
DANMAKU_PORT = 8601
HEARTBEAT_INTERVAL = 45

# 礼物入库阈值 (鱼翅)。catalog 已知价格 < 此值 → drop, 不入 DB 也不广播。
# price_yuchi=None (catalog 未收录的新礼物) 一律保留作为安全网。
# 设为 6 与前端默认 gift_threshold_yuchi 一致 — 主播看不到的就不存。
STORE_THRESHOLD_YUCHI = 6.0

# 订阅类事件 (钻粉/贵族开通) 实测会在多个 gid 通道重复广播,导致同秒入库两条。
# 注意斗鱼对同一次钻粉操作还会下发两种 type 副本: dfobc/odfpbc (开通含/不含 price)
# 与 dfrbc/rdfpbc (续费含/不含 price) — 必须按"事件组"折,而不是按 type 折,
# 否则同 uid 同秒会留下双份 (实测 2026-05-09 #1830 dfrbc + #1831 rdfpbc)。
# 5s 窗口先到先得; LRU 大小 256 足够覆盖瞬时高峰。
_DEDUP_GROUP = {
    "dfobc":  "df_open",   # 开通钻粉 (含 price)
    "odfpbc": "df_open",   # 开通钻粉 (无 price, 双发副本)
    "dfrbc":  "df_renew",  # 续费钻粉 (含 price)
    "rdfpbc": "df_renew",  # 续费钻粉 (无 price, 双发副本)
    "anbc":   "anbc",      # 贵族开通 — 暂未观察到双发, 各自独立
    "rnewbc": "rnewbc",    # 贵族续费
}
_DEDUP_WINDOW_MS = 5000
_DEDUP_LRU_CAP = 256

# 未识别 type 的 body 写到此文件供后续分析；每个 type 最多 _DIAG_SAMPLES 条样本
_DIAG_LOG_PATH = PROJECT_DIR / "diag.log"
_DIAG_SAMPLES = 5

# 已知 0 价值粉丝团/活动连刷物。低位 gfid, v3 catalog 不收录, 实测从未带任何价值字段。
# 生产 10 天数据 (events.db 17375 行) 里这 4 种占 totally_unknown 礼物的 ~97%:
#   陪伴印章(3410): 4474 events / 191万 units
#   粉丝荧光棒(824/1914): 4252 events
#   钻粉荧光棒(22899): 2619 events
#   星光棒(3567): 853 events
# 提前 drop 避免 DB 被刷屏; 主播界面的核心信号 (鱼翅/亲密度大礼) 不受影响。
# 想撤销某条 gfid 拉黑, 从此集合移除即可, 不需要其他改动。
_ZERO_VALUE_GIFT_IDS: frozenset[int] = frozenset({
    3410,   # 陪伴印章
    824,    # 粉丝荧光棒
    1914,   # 粉丝荧光棒 (variant)
    22899,  # 钻粉荧光棒
    3567,   # 星光棒
})

# Douyu splits chat across multiple "groups" on busy rooms; joining several
# yields ~40% more chatmsg than the broadcast group alone (verified A/B).
GIDS_TO_JOIN: tuple[int, ...] = (-9999, 1, 2, 3, 4, 5)

# Source type strings (server) → our normalized 'kind'
# Subscription type names confirmed via [diag] log:
#   2026-05-08: dfobc 开通钻粉 (验证: 1580 鱼翅事件) / dfrbc 推测的续费版
#   2026-05-09: odfpbc / rdfpbc — 另一种钻粉广播 (无 price 字段, 疑似赠送或活动型)
# 贵族 anbc/rnewbc 仍未在本环境触发，保留占位。
# 旧的 odfbc/rndfbc 在本环境从未触发已移除。
_KIND_MAP = {
    "dgb":             "gift",
    "ssd":             "superchat",     # placeholder, may not actually fire
    "comm_chatmsg":    "superchat",     # 高能弹幕 V2 — paid voice/text barrage
    "chatmsg":         "chat",          # ephemeral, NOT persisted
    "dfobc":           "subscription",  # 开通钻粉 (with price)
    "dfrbc":           "subscription",  # 续费钻粉 (speculative)
    "odfpbc":          "subscription",  # 开通钻粉 (no price; 赠送/活动型)
    "rdfpbc":          "subscription",  # 续费钻粉 (no price)
    "anbc":            "subscription",  # 开通贵族 (untested)
    "rnewbc":          "subscription",  # 续费贵族 (untested)
    "rec_barrage_hot": "hot_barrage",   # 斗鱼聚合的"N 人在说 XXX"热度帧 (ephemeral)
    "oni":             "vip_info",      # 在线贵宾数推送 (vn 字段); 每 ~5s 一帧, ephemeral
}

# 贵族等级名 (per douyu-monitor: vue/src/global/utils/dydata/nobleData.js)
_NOBLE_NAMES = {
    1: "骑士", 2: "子爵", 3: "伯爵", 4: "公爵", 5: "国王",
    6: "皇帝", 7: "诸侯",
}

OnEvent = Callable[[dict], Awaitable[None]]
GiftCatalogRefresher = Callable[[], Awaitable[Dict[int, dict]]]

# Diagnostic: types that show up every second and would spam the log if surfaced.
# Anything NOT in this set and NOT in _KIND_MAP gets a one-shot INFO log on
# first sight, so when Douyu introduces a new event (e.g. 钻粉广播 type changes)
# we can spot it without running sniff.py separately.
_DIAG_NOISE = {
    # 已知的高频协议噪音
    "chatmsg", "uenter", "oun", "mrkl", "pingreq", "loginres",
    "actFishing", "defense_tower_session", "synexp", "rtss_update",
    "ranklist", "anchor_rank2505_change", "configscreen",
    "blab", "online_noble_list", "rss", "noble_num_info", "frank",
    # 2026-05-08 排查 diag.log 确认无价值或与现有信号重复:
    "spbc",        # 跨房间大礼物广播 (drid 不是当前房间; 本房间的礼物 dgb 已覆盖)
    "cthn",        # 跨房间彩色弹幕广播
    "voice_trlt",  # 语音高能弹幕的 mp3 翻译广播; Phase 4 做 mp3 归档时再恢复
    "upgrade",     # 用户等级升级
    "tsboxb",      # 任务系统-宝箱事件 (rpt 是任务点不是鱼翅)
    "tsgs",        # 任务系统-观众间礼物
    # 2026-05-09 Phase 2 采样后追加:
    "dfnum",            # 粉丝数变化通知
    "srres",            # 系统响应/确认帧
    "dyh_legend_seas",  # 主播大乱斗活动状态
    "little_lucky_info", "pocketTips",  # 幸运/口袋活动
    "fxdaychange",      # 活动每日重置
    "anchor_rights",    # 主播权益变更
    "rankup",           # 用户房间排名上升
    "tsd",              # 任务完成通知
    "rtss_complete",    # 任务奖励发放
    "dfv2", "dfv2_pd_sw",  # 钻粉 v2 卡牌/段位
    "growth_wel_banner",   # 用户成长欢迎横幅
    "newblackres",         # 黑名单/禁言操作回执 (含 sid/did/endtime), 与礼物无关
}


class Collector:
    def __init__(
        self,
        room_id: int,
        on_event: OnEvent,
        gift_catalog: Dict[int, dict] | None = None,
        pandora_catalog: Dict[int, dict] | None = None,
        gift_catalog_refresher: GiftCatalogRefresher | None = None,
        catalog_refresh_min_interval: float = 300.0,
        pandora_catalog_refresher: GiftCatalogRefresher | None = None,
    ):
        self.room_id = room_id
        self.on_event = on_event
        self.gift_catalog: Dict[int, dict] = gift_catalog or {}
        self.pandora_catalog: Dict[int, dict] = pandora_catalog or {}
        self.gift_catalog_refresher = gift_catalog_refresher
        self.pandora_catalog_refresher = pandora_catalog_refresher
        self.catalog_refresh_min_interval = catalog_refresh_min_interval
        self._last_catalog_refresh = 0.0
        self._last_pandora_refresh = 0.0
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._unknown_counts: dict[str, int] = {}
        self._diag_fp = None
        self._dedup: "OrderedDict[tuple, int]" = OrderedDict()

    def _gift_meta(self, gfid: int | None, pid: int | None) -> dict:
        meta = self.gift_catalog.get(gfid) if gfid else None
        if not meta and pid:
            meta = self.gift_catalog.get(pid)
        return meta or {}

    async def _refresh_gift_catalog_if_needed(self) -> None:
        if not self.gift_catalog_refresher:
            return
        now = time.monotonic()
        if now - self._last_catalog_refresh < self.catalog_refresh_min_interval:
            return
        self._last_catalog_refresh = now
        try:
            catalog = await self.gift_catalog_refresher()
        except Exception as e:
            log.warning("gift catalog refresh failed: %s", e)
            return
        if catalog:
            self.gift_catalog = catalog
            log.info("refreshed gift catalog: %d gifts for room %d", len(catalog), self.room_id)

    async def _refresh_pandora_catalog_if_needed(self) -> None:
        """乾坤袋 award 池刷新: 与主 catalog 同节奏 (默认 5min), 用于斗鱼新开活动后
        award pool 增删的场景。失败时保留旧 catalog 不抛错。"""
        if not self.pandora_catalog_refresher:
            return
        now = time.monotonic()
        if now - self._last_pandora_refresh < self.catalog_refresh_min_interval:
            return
        self._last_pandora_refresh = now
        try:
            catalog = await self.pandora_catalog_refresher()
        except Exception as e:
            log.warning("pandora catalog refresh failed: %s", e)
            return
        if catalog:
            self.pandora_catalog = catalog
            log.info("refreshed pandora catalog: %d pids for room %d", len(catalog), self.room_id)

    def _is_duplicate(self, t: str, kv: dict, ts_ms: int) -> bool:
        """Suppress same-(group, uid) events within DEDUP_WINDOW_MS for known-noisy types."""
        group = _DEDUP_GROUP.get(t)
        if group is None:
            return False
        uid = str(kv.get("uid") or kv.get("src_uid") or "")
        if not uid:
            return False  # no uid, can't dedup safely
        key = (group, uid)
        last = self._dedup.get(key)
        if last is not None and ts_ms - last < _DEDUP_WINDOW_MS:
            self._dedup.move_to_end(key)
            return True
        self._dedup[key] = ts_ms
        self._dedup.move_to_end(key)
        if len(self._dedup) > _DEDUP_LRU_CAP:
            self._dedup.popitem(last=False)
        return False

    def start(self) -> None:
        self._task = asyncio.create_task(self._run_forever(), name=f"douyu-{self.room_id}")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        if self._diag_fp:
            try:
                self._diag_fp.close()
            except Exception:
                pass
            self._diag_fp = None

    def _log_unknown(self, t: str, body: str) -> None:
        n = self._unknown_counts.get(t, 0) + 1
        self._unknown_counts[t] = n
        if n == 1:
            log.info("[diag] first-seen unknown type: %s, body[:200]: %s", t, body[:200])
        if n > _DIAG_SAMPLES:
            return
        if self._diag_fp is None:
            try:
                fp = open(_DIAG_LOG_PATH, "a", encoding="utf-8", buffering=1)
                if fp.tell() == 0:
                    fp.write(
                        "# Douyu unknown-type diag log\n"
                        "# format: <ISO time>\\t<type>\\t<full body>\n"
                        f"# up to {_DIAG_SAMPLES} samples per type per server process\n"
                    )
                self._diag_fp = fp
            except OSError as e:
                log.warning("diag log open failed: %s", e)
                return
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        try:
            self._diag_fp.write(f"{ts}\t{t}\t{body}\n")
        except Exception as e:
            log.warning("diag log write failed: %s", e)

    async def _run_forever(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._run_once()
                backoff = 1.0  # reset on clean disconnect
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("collector error: %s; reconnect in %.1fs", e, backoff)
            if self._stop.is_set():
                return
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    async def _run_once(self) -> None:
        log.info("connecting to %s:%d for room %d", DANMAKU_HOST, DANMAKU_PORT, self.room_id)
        reader, writer = await asyncio.open_connection(DANMAKU_HOST, DANMAKU_PORT)
        try:
            writer.write(proto.login_req(self.room_id))
            for gid in GIDS_TO_JOIN:
                writer.write(proto.join_group(self.room_id, gid))
            await writer.drain()
            log.info("login + joingroup(%s) sent", list(GIDS_TO_JOIN))

            hb_task = asyncio.create_task(self._heartbeat_loop(writer))
            try:
                await self._read_loop(reader)
            finally:
                hb_task.cancel()
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _heartbeat_loop(self, writer: asyncio.StreamWriter) -> None:
        try:
            while not self._stop.is_set():
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                writer.write(proto.heartbeat())
                await writer.drain()
        except (asyncio.CancelledError, ConnectionError):
            return

    async def _read_loop(self, reader: asyncio.StreamReader) -> None:
        buf = bytearray()
        while not self._stop.is_set():
            chunk = await reader.read(8192)
            if not chunk:
                log.info("server closed connection")
                return
            buf.extend(chunk)
            for _msg_type, body in proto.iter_frames(buf):
                kv = proto.parse_kv(body)
                t = kv.get("type")
                if t in _KIND_MAP:
                    await self._dispatch(t, kv, body)
                elif t and t not in _DIAG_NOISE:
                    self._log_unknown(t, body)

    async def _dispatch(self, t: str, kv: dict, raw: str) -> None:
        kind = _KIND_MAP[t]
        ts_ms = int(time.time() * 1000)
        if self._is_duplicate(t, kv, ts_ms):
            return
        if kind == "gift":
            gfid = _to_int(kv.get("gfid"))
            if gfid in _ZERO_VALUE_GIFT_IDS:
                return  # 已知 0 价值粉丝团连刷物, 不入 DB 不广播
            pid = _to_int(kv.get("pid"))
            count = _to_int(kv.get("gfcnt")) or 1
            # name resolution: gfn (in payload) > catalog[gfid|pid] > pandora[pid] > placeholder
            name = kv.get("gfn") or None
            meta = self._gift_meta(gfid, pid)
            if not meta and (gfid or pid):
                await self._refresh_gift_catalog_if_needed()
                meta = self._gift_meta(gfid, pid)
            # 主 catalog 没收录但是乾坤袋抽中礼物 (from=2, pid 在奖品池中) - fallback 到 pandora
            pandora_meta = self.pandora_catalog.get(pid) if (pid and not meta) else None
            # 主 catalog & pandora 双 miss 且有 pid → 触发 pandora 刷新一次再查
            # (斗鱼新开 pandora 活动时, award pool 会出现新 pid)
            if pid and not meta and not pandora_meta:
                await self._refresh_pandora_catalog_if_needed()
                pandora_meta = self.pandora_catalog.get(pid)
            if not name:
                name = meta.get("name") or (pandora_meta.get("name") if pandora_meta else None)
            if not name:
                name = f"礼物#{gfid}" if gfid else (f"礼物#pid={pid}" if pid else "未知礼物")
            price_yuchi = meta.get("price_yuchi") or 0.0   # YUCHI 真值, 否则 0
            price_yuwan = meta.get("price_yuwan") or 0.0   # YUWAN 真值, 否则 0
            # 单位亲密度: 主 catalog 的 intimacy (普通礼物自带), 否则 pandora 的 intimacy_per_unit (乾坤袋抽中)
            intimacy = (
                meta.get("intimacy") or 0
                or (pandora_meta.get("intimacy_per_unit") if pandora_meta else 0)
            )
            price_type = meta.get("price_type")  # None = 主 catalog 未收录 (可能 pandora 收录)
            # 等效鱼翅 (单位): 鱼翅本身 + 亲密度 ÷10。鱼丸不折算 (走亲密度路径或被阈值杀)。
            effective_yuchi = max(price_yuchi, intimacy / 10.0)
            # 阈值过滤: 已知低价值 → drop; 完全未收录 (主 catalog & pandora 都没有) → 保留作安全网
            is_known = (price_type is not None) or (pandora_meta is not None)
            if is_known and effective_yuchi * count < STORE_THRESHOLD_YUCHI:
                return
            # DB price_yuchi 只存真鱼翅 (避免亲密度/鱼丸污染统计总额)
            db_price = price_yuchi if price_yuchi > 0 else None
            event = {
                "ts": ts_ms,
                "room_id": self.room_id,
                "kind": "gift",
                "uid": kv.get("uid") or kv.get("src_uid"),
                "nickname": kv.get("nn") or kv.get("src_ncnm"),
                "gift_id": gfid if gfid else pid,
                "gift_name": name,
                "count": count,
                "price_yuchi": db_price,
                "content": None,
                "raw": raw,
                # 以下字段不入 DB (db.insert 不读取),仅随 WS payload 推给前端做特殊渲染:
                "intimacy":        intimacy or None,            # 单位亲密度
                "price_yuwan":     price_yuwan if price_yuwan > 0 else None,
                "price_type":      price_type,
                "effective_yuchi": effective_yuchi,             # 单位等效鱼翅 (前端乘 count)
            }
        elif kind == "superchat" and t == "comm_chatmsg":
            # 高能弹幕 V2: outer carries vrid/btype/cprice/cet, inner chatmsg@= holds user data.
            # Inner uses level-1 escaping: @S→/ @A→@ — already handled by parse_kv in the outer pass.
            inner_body = kv.get("chatmsg") or ""
            inner = proto.parse_kv(inner_body) if inner_body else {}
            cprice_centi = _to_int(kv.get("cprice")) or _to_int(kv.get("crealPrice")) or 0
            # btype=pandora 等"零价值高能"复用了 comm_chatmsg 协议但 cprice=0
            # (潘多拉宝箱抽中礼物的全房广播，那笔钱在买宝箱时已计入 dgb)。
            # 丢弃以避免重复信号。
            if cprice_centi <= 0:
                return
            event = {
                "ts": ts_ms,
                "room_id": self.room_id,
                "kind": "superchat",
                "uid": inner.get("uid") or kv.get("uid"),
                "nickname": inner.get("nn"),
                "gift_id": None,
                "gift_name": kv.get("btype") or "高能弹幕",  # e.g. 'voiceDanmu' / '高能弹幕'
                "count": 1,
                "price_yuchi": (cprice_centi / 100.0) if cprice_centi else None,
                "content": inner.get("txt"),
                "color": None,
                "raw": raw,
            }
        elif kind == "superchat":  # legacy ssd path
            event = {
                "ts": ts_ms,
                "room_id": self.room_id,
                "kind": "superchat",
                "uid": kv.get("suid") or kv.get("senduid") or kv.get("uid"),
                "nickname": kv.get("snic") or kv.get("nn"),
                "gift_id": None,
                "gift_name": None,
                "count": 1,
                "price_yuchi": _to_int(kv.get("sdpr")),
                "content": kv.get("content") or kv.get("trd"),
                "color": None,
                "raw": raw,
            }
        elif kind == "subscription":
            # 钻粉广播有四种 type，字段大同小异：
            #   dfobc / dfrbc  — 自购，含 price (RMB 分；÷10 = 鱼翅)
            #   odfpbc / rdfpbc — 无 price (赠送/活动型)
            # 共用字段: uid / nick / mn(月数) / rrid(目标主播房间)
            if t in ("dfobc", "dfrbc", "odfpbc", "rdfpbc"):
                rrid = _to_int(kv.get("rrid"))
                if rrid and rrid != self.room_id:
                    return  # cross-room broadcast
                is_open = t in ("dfobc", "odfpbc")
                price_cents = _to_int(kv.get("price")) or 0
                price_yuchi = price_cents / 10.0 if price_cents else None
                mn = _to_int(kv.get("mn")) or 1
                label = "开通钻粉" if is_open else "续费钻粉"
                if mn > 1:
                    label = f"{label} ×{mn} 月"
                event = {
                    "ts": ts_ms,
                    "room_id": self.room_id,
                    "kind": "subscription",
                    "uid": kv.get("uid"),
                    "nickname": kv.get("nick") or kv.get("nn"),
                    "gift_id": None,
                    "gift_name": label,
                    "count": 1,
                    "price_yuchi": price_yuchi,
                    "content": kv.get("bn") or None,
                    "color": None,
                    "raw": raw,
                }
            else:  # anbc / rnewbc — 贵族开通/续费 (untested branch)
                drid = _to_int(kv.get("drid"))
                if drid and drid != self.room_id:
                    return
                nl = _to_int(kv.get("nl"))
                label = {
                    "anbc":   f"开通{_NOBLE_NAMES.get(nl or 0, '贵族')}",
                    "rnewbc": f"续费{_NOBLE_NAMES.get(nl or 0, '贵族')}",
                }[t]
                event = {
                    "ts": ts_ms,
                    "room_id": self.room_id,
                    "kind": "subscription",
                    "uid": kv.get("uid"),
                    "nickname": kv.get("nick") or kv.get("unk") or kv.get("nn"),
                    "gift_id": nl,
                    "gift_name": label,
                    "count": 1,
                    "price_yuchi": None,
                    "content": None,
                    "raw": raw,
                }
        elif kind == "vip_info":
            # type@=oni: 在线贵宾推送, vn=贵宾数, un=同义副本
            # ephemeral, 不入库, 仅广播
            vn = _to_int(kv.get("vn"))
            if vn is None:
                return
            event = {
                "ts": ts_ms,
                "room_id": self.room_id,
                "kind": "vip_info",
                "vip_count": vn,
                "uid": None, "nickname": None, "gift_id": None,
                "gift_name": None, "count": None, "price_yuchi": None,
                "content": None, "color": None, "raw": None,
            }
        elif kind == "hot_barrage":
            # rec_barrage_hot.content 是 JSON: {"bg": 完整原句, "hot": 热点关键词, "showTime":10, "total": N}
            # 与"N 人在说 hot"对应。ephemeral，不入库。
            try:
                info = json.loads(kv.get("content") or "{}")
            except Exception:
                return
            event = {
                "ts": ts_ms,
                "room_id": self.room_id,
                "kind": "hot_barrage",
                "uid": None,
                "nickname": None,
                "gift_id": None,
                "gift_name": info.get("hot") or "",   # 复用字段：热点关键词
                "count": _to_int(info.get("total")) or 1,
                "price_yuchi": None,
                "content": info.get("bg") or "",      # 复用字段：完整原句
                "color": None,
                "raw": None,
            }
        else:  # chatmsg — always ephemeral. col/nl mean noble/diamond-fan styling, NOT SuperChat.
            col_raw = kv.get("col") or ""
            try:
                color = int(col_raw) or None
            except (TypeError, ValueError):
                color = None
            event = {
                "ts": ts_ms,
                "room_id": self.room_id,
                "kind": "chat",
                "uid": kv.get("uid"),
                "nickname": kv.get("nn"),
                "gift_id": None,
                "gift_name": None,
                "count": 1,
                "price_yuchi": None,
                "content": kv.get("txt"),
                "color": color,  # passed through for chat-panel tinting only
                "raw": None,
            }
        try:
            await self.on_event(event)
        except Exception as e:
            log.exception("on_event handler failed: %s", e)


def _to_int(v) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
