"""Forensic monitor for Douyu's high-energy danmaku.

Launches a Chromium with a persistent profile (so you can stay logged in if
needed), navigates to the room, and INJECTS a hook *before* any page script
runs. The hook captures:

  • every WebSocket frame received (binary or blob), decoded into KV body
  • every relevant HTTP response (athena/barrage, getLiveLoopBarrage, ...)
  • every DOM mutation inside chat-related containers, with snapshots

Everything is dumped to:

  monitor_<rid>_<startms>.jsonl    one event per line

Usage:
  python -m hyacinth_sentry.playwright_monitor 12740109
  # browse normally, watch the room, and when you see a SC card flash, look
  # in the JSONL for events near that time.

Quit with Ctrl-C.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

# `playwright` is a separate dependency from this project.
try:
    from playwright.async_api import async_playwright
except ImportError:
    sys.exit("playwright not installed. Run: python -m pip install playwright "
             "&& python -m playwright install chromium")


INIT_JS = r"""
// Runs BEFORE any page script. Installs WS hook + observer skeleton. The
// page evaluates this in a fresh execution context per navigation, so we
// must keep state on window.* — Python polls it via `evaluate`.
(() => {
  if (window.__dy_mon_installed) return;
  window.__dy_mon_installed = true;

  // --- ring buffer with size cap ---
  const MAX_QUEUE = 5000;
  window.__dy_mon_queue = [];
  function emit(kind, data) {
    window.__dy_mon_queue.push({ ts: Date.now(), kind, data });
    if (window.__dy_mon_queue.length > MAX_QUEUE) {
      window.__dy_mon_queue.splice(0, window.__dy_mon_queue.length - MAX_QUEUE);
    }
  }
  window.__dy_mon_drain = () => {
    const q = window.__dy_mon_queue;
    window.__dy_mon_queue = [];
    return q;
  };

  // --- WebSocket hook ---
  const OrigWS = window.WebSocket;
  function decodeFrames(buf) {
    const view = new DataView(buf);
    const out = [];
    let off = 0;
    while (off + 12 <= buf.byteLength) {
      const total = view.getUint32(off, true);
      const frameSize = total + 4;
      if (off + frameSize > buf.byteLength) break;
      const bytes = new Uint8Array(buf, off + 12, frameSize - 12);
      let end = bytes.length;
      while (end > 0 && bytes[end - 1] === 0) end--;
      out.push(new TextDecoder("utf-8").decode(bytes.subarray(0, end)));
      off += frameSize;
    }
    return out;
  }
  function H(...args) {
    const ws = new OrigWS(...args);
    emit("ws.open", { url: String(args[0] || "") });
    ws.addEventListener("close", (e) =>
      emit("ws.close", { code: e.code, reason: e.reason }));
    ws.addEventListener("message", async (ev) => {
      try {
        const d = ev.data;
        let buf = null;
        if (d instanceof ArrayBuffer) buf = d;
        else if (d && typeof d.arrayBuffer === "function") buf = await d.arrayBuffer();
        else if (d && d.buffer) buf = d.buffer;
        else { emit("ws.unknown", { t: typeof d }); return; }
        for (const body of decodeFrames(buf)) {
          const m = body.match(/^type@=([^/]+)/);
          emit("ws.frame", { type: m ? m[1] : "?", body });
        }
      } catch (e) {
        emit("ws.err", { msg: String(e) });
      }
    });
    return ws;
  }
  H.prototype = OrigWS.prototype;
  for (const k of ["CONNECTING", "OPEN", "CLOSING", "CLOSED"]) H[k] = OrigWS[k];
  window.WebSocket = H;

  // --- DOM observer, attached after DOMContentLoaded ---
  function snap(el) {
    if (!el) return null;
    return {
      cls: el.className || "",
      tag: el.tagName,
      text: (el.textContent || "").replace(/\s+/g, " ").trim().slice(0, 600),
      html: (el.outerHTML || "").slice(0, 1500),
    };
  }
  function watch(selector, label) {
    const el = document.querySelector(selector);
    if (!el) return false;
    emit("dom.attach", { selector, label });
    new MutationObserver((mutations) => {
      for (const m of mutations) {
        for (const node of m.addedNodes) {
          if (node.nodeType !== 1) continue;
          emit("dom.add", { sel: selector, label, node: snap(node) });
        }
      }
    }).observe(el, { childList: true, subtree: true });
    return true;
  }
  function attachAll() {
    const targets = [
      [".Barrage-topFloater",     "topFloater (顶部浮动卡片)"],
      ["#js-floatingbarrage-container", "floatingbarrageContainer"],
      [".Barrage-preview",        "preview (预览卡片)"],
      ["#js-barrage-preview-wrap", "previewWrap"],
      [".BarrageBanner",          "banner"],
      [".BarrageGiftBannerList",  "giftBannerList"],
      [".DanmuEffectDom",         "effectDom"],
      [".Barrage-list",           "barrageList (用作普通弹幕基线)"],
    ];
    let attached = 0;
    for (const [sel, label] of targets) {
      if (watch(sel, label)) attached++;
    }
    emit("dom.attached", { count: attached });
    return attached;
  }
  // Try every 500ms for up to 30s — Vue mounts asynchronously.
  let tries = 0;
  const iv = setInterval(() => {
    tries++;
    const n = attachAll();
    if (n >= 6 || tries > 60) clearInterval(iv);
  }, 500);
})();
"""


async def main(rid: int, profile: Path, out_path: Path) -> None:
    profile.mkdir(parents=True, exist_ok=True)
    print(f"profile: {profile}")
    print(f"output:  {out_path}")
    print(f"target:  https://www.douyu.com/{rid}")
    print("浏览器即将启动；像平时一样观看直播，关掉窗口或 Ctrl-C 结束采集。\n")

    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(
            str(profile),
            headless=False,
            viewport=None,
            args=["--start-maximized"],
        )
        await ctx.add_init_script(INIT_JS)

        # Capture HTTP responses for the suspicious endpoints.
        out = open(out_path, "w", encoding="utf-8", buffering=1)

        def write(kind: str, data: dict) -> None:
            out.write(json.dumps({"ts": int(time.time() * 1000), "kind": kind, **data},
                                 ensure_ascii=False) + "\n")

        async def on_response(resp):
            url = resp.url
            if not any(k in url for k in ("/lapi/athena/", "/barrage/", "barrage.json",
                                          "/japi/", "/wgapi/")):
                return
            try:
                body = await resp.text()
            except Exception:
                body = "<unreadable>"
            write("http", {"url": url, "status": resp.status, "body": body[:4000]})

        ctx.on("response", lambda r: asyncio.create_task(on_response(r)))

        page = await ctx.new_page()
        page.on("console", lambda msg: write("console", {"type": msg.type, "text": msg.text}))
        await page.goto(f"https://www.douyu.com/{rid}")

        # Drain the in-page queue every second.
        while True:
            try:
                await asyncio.sleep(1.0)
                events = await page.evaluate("() => window.__dy_mon_drain ? window.__dy_mon_drain() : []")
            except Exception as e:
                # Page closed or context died.
                write("monitor.exit", {"reason": str(e)})
                break
            for ev in events:
                write("page." + ev["kind"], {"_pts": ev["ts"], **(ev.get("data") or {})})

        out.close()


def cli() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("rid", type=int, help="Douyu room id, e.g. 12740109")
    ap.add_argument("--profile", default=str(Path(__file__).parent.parent / "playwright_profile"),
                    help="Persistent Chromium profile dir (so login stays put)")
    ap.add_argument("--out", default=None, help="JSONL output path")
    args = ap.parse_args()
    out = Path(args.out) if args.out else (
        Path(__file__).parent.parent / f"monitor_{args.rid}_{int(time.time())}.jsonl"
    )
    try:
        asyncio.run(main(args.rid, Path(args.profile), out))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
