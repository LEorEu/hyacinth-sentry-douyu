"""Douyu danmaku TCP protocol: framing + KV body codec."""
from __future__ import annotations

import struct
from typing import Dict

# Message types
CLIENT_TO_SERVER = 689
SERVER_TO_CLIENT = 690

# Header: total_len(4) total_len(4) type(2) encrypt(1) reserved(1)
_HEADER = struct.Struct("<IIHBB")


def encode(body: str) -> bytes:
    """Encode a KV body string as a single Douyu protocol frame."""
    payload = body.encode("utf-8") + b"\x00"
    total_len = 4 + 2 + 1 + 1 + len(payload)  # excludes the leading 4-byte length itself
    return _HEADER.pack(total_len, total_len, CLIENT_TO_SERVER, 0, 0) + payload


def iter_frames(buf: bytearray):
    """Yield (msg_type, body_str) pairs from a buffer; mutates buf to consume."""
    while True:
        if len(buf) < 12:
            return
        total_len = struct.unpack_from("<I", buf, 0)[0]
        frame_size = total_len + 4
        if len(buf) < frame_size:
            return
        _, _, msg_type, _, _ = _HEADER.unpack_from(buf, 0)
        body = bytes(buf[12:frame_size]).rstrip(b"\x00")
        del buf[:frame_size]
        try:
            yield msg_type, body.decode("utf-8", errors="replace")
        except Exception:
            continue


def parse_kv(body: str) -> Dict[str, str]:
    """Parse `key@=value/key@=value/` body. Unescapes @S → / and @A → @ in values."""
    out: Dict[str, str] = {}
    for part in body.split("/"):
        if not part or "@=" not in part:
            continue
        k, _, v = part.partition("@=")
        out[k] = v.replace("@S", "/").replace("@A", "@")
    return out


# Convenience builders
def login_req(room_id: int) -> bytes:
    return encode(f"type@=loginreq/roomid@={room_id}/")


def join_group(room_id: int, gid: int = -9999) -> bytes:
    return encode(f"type@=joingroup/rid@={room_id}/gid@={gid}/")


def heartbeat() -> bytes:
    return encode("type@=mrkl/")
