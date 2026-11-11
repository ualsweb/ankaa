"""A small simulated plant publishing to an MQTT Unified Namespace.

Topics follow an ISA-95 hierarchy:  <enterprise>/<site>/<area>/<line>/<machine>/telemetry
Payload (JSON):                       {"t": <epoch ms>, "kw": 12.34, "temp_c": 41.2, "state": 1}

What a real plant does, and so does this one:
- about 2 % of readings come without "kw" (a missing value, stored as NULL), about 1 % with
  "temp_c": null;
- the gateway of line 4 drops off for 60 s every 3 minutes, buffers, then sends everything late
  (the table accepts readings up to 5 minutes late and places them in time);
- robot09 stops for a minute every 10 minutes (state 0, near-zero power);
- press03 draws 25 % more power for two minutes every quarter of an hour (the anomaly to find).

Environment: MQTT_HOST (default "mqtt"), MQTT_PORT (1883), RATE_HZ (readings per machine per
second, default 1), PREFIX (default "acme/plant1").
"""

import json
import math
import os
import random
import time

import paho.mqtt.client as mqtt

HOST = os.environ.get("MQTT_HOST", "mqtt")
PORT = int(os.environ.get("MQTT_PORT", "1883"))
RATE_HZ = float(os.environ.get("RATE_HZ", "1"))
PREFIX = os.environ.get("PREFIX", "acme/plant1")

PLANT = {
    "press-shop": {
        "line1": ["press01", "press02", "press03", "press04"],
        "line2": ["press05", "press06", "press07", "press08"],
    },
    "assembly": {
        "line3": ["robot01", "robot02", "robot03", "robot04", "robot05", "robot06"],
        "line4": ["robot07", "robot08", "robot09", "robot10", "robot11", "robot12"],
    },
}
BUFFERING_LINE = "line4"

rng = random.Random(42)
machines = []
for area, lines in PLANT.items():
    for line, names in lines.items():
        for name in names:
            base = 55.0 if area == "press-shop" else 9.0
            machines.append(
                {
                    "topic": f"{PREFIX}/{area}/{line}/{name}/telemetry",
                    "line": line,
                    "name": name,
                    "base": base * rng.uniform(0.85, 1.15),
                    "phase": rng.uniform(0, 2 * math.pi),
                    "temp": rng.uniform(30, 40),
                }
            )


def reading(m, now_s):
    """One reading of machine m at time now_s (seconds)."""
    minute = now_s / 60.0
    running = not (m["name"] == "robot09" and int(minute) % 10 == 0)
    kw = m["base"] * (1.0 + 0.12 * math.sin(now_s / 7.0 + m["phase"])) + rng.gauss(0, m["base"] * 0.02)
    if m["name"] == "press03" and (minute % 15) < 2:
        kw *= 1.25
    if not running:
        kw = m["base"] * 0.03
    m["temp"] += (0.02 if running else -0.05) + rng.gauss(0, 0.01)
    m["temp"] = min(max(m["temp"], 25.0), 75.0)
    msg: dict[str, object] = {"t": int(now_s * 1000), "state": 1 if running else 0}
    if rng.random() >= 0.02:
        msg["kw"] = round(max(kw, 0.0), 3)
    msg["temp_c"] = None if rng.random() < 0.01 else round(m["temp"], 2)
    return msg


def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="plant1-simulator")
    while True:
        try:
            client.connect(HOST, PORT, keepalive=30)
            break
        except OSError as e:
            print(f"waiting for the broker at {HOST}:{PORT}: {e}", flush=True)
            time.sleep(2)
    client.loop_start()
    print(f"publishing {len(machines)} machines at {RATE_HZ} Hz to {PREFIX}/...", flush=True)
    buffered = []
    period = 1.0 / RATE_HZ
    next_tick = time.time()
    sent = 0
    while True:
        now = time.time()
        offline = (now / 60.0) % 3 < 1  # the line-4 gateway is offline one minute in three
        for m in machines:
            payload = (m["topic"], json.dumps(reading(m, now)))
            if m["line"] == BUFFERING_LINE and offline:
                buffered.append(payload)
            else:
                client.publish(*payload, qos=1)
                sent += 1
        if buffered and not offline:
            for topic, body in buffered:  # the gateway is back: everything it held, late
                client.publish(topic, body, qos=1)
                sent += 1
            print(f"line 4 gateway back online: sent {len(buffered)} buffered readings", flush=True)
            buffered = []
        if sent and sent % (len(machines) * 60) < len(machines):
            print(f"{sent} readings published", flush=True)
        next_tick += period
        time.sleep(max(0.0, next_tick - time.time()))


if __name__ == "__main__":
    main()
