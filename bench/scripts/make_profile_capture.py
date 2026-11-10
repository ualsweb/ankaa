#!/usr/bin/env python3
"""Build a large, time-shifted capture for profiling from a small real one.

Repeats every text frame N times, shifting Binance event/trade times (ms), trade ids and depth update ids
per copy so the result is a long, in-order, gap-free stream. Output is a valid .ankc file (CRC32C per frame).

usage: make_profile_capture.py <in.ankc> <out.ankc> <copies>
"""
import json, struct, sys

TABLE = []
for i in range(256):
    c = i
    for _ in range(8):
        c = (c >> 1) ^ 0x82F63B78 if c & 1 else c >> 1
    TABLE.append(c)

def crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for b in data:
        crc = TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF

def main(src, dst, copies):
    b = open(src, 'rb').read()
    assert b[:4] == b'ANKC'
    n = struct.unpack_from('<H', b, 8)[0]
    off = 10 + n + 6
    header = b[:off]
    frames = []
    while off < len(b):
        recv, kind, _r, ln = struct.unpack_from('<qBBI', b, off)
        payload = b[off + 14: off + 14 + ln]
        frames.append((recv, kind, payload))
        off += 14 + ln + 4
    text = [(r, p) for r, k, p in frames if k == 1]
    span_ns = text[-1][0] - text[0][0] + 1_000_000
    span_ms = span_ns // 1_000_000
    max_u = max(json.loads(p)['data'].get('u', 0) for _, p in text)
    max_t = max(json.loads(p)['data'].get('t', 0) for _, p in text)
    out = bytearray(header)
    for k in range(copies):
        for recv, p in text:
            j = json.loads(p)
            d = j['data']
            for f in ('E', 'T'):
                if f in d:
                    d[f] += k * span_ms
            for f in ('U', 'u'):
                if f in d:
                    d[f] += k * (max_u + 1)
            if 't' in d:
                d['t'] += k * (max_t + 1)
            payload = json.dumps(j, separators=(',', ':')).encode()
            rec = struct.pack('<qBBI', recv + k * span_ns, 1, 0, len(payload)) + payload
            out += rec + struct.pack('<I', crc32c(rec))
    open(dst, 'wb').write(out)
    print(f"frames={len(text) * copies} bytes={len(out)}")

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], int(sys.argv[3]))
