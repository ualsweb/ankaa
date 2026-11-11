"""Resource probes for bench/telemetry: memory, CPU time and on-disk size of one engine.

Containers (Linux, cgroup v2): read straight from the container's cgroup on the host
(`/sys/fs/cgroup/system.slice/docker-<id>.scope/` with the systemd driver, `/sys/fs/cgroup/docker/<id>/`
with the cgroupfs driver):
  memory.peak     highest charge since the container started (anonymous memory AND page cache the
                  container caused, e.g. files it wrote): an upper bound, reported as `cgroup_peak_bytes`
  memory.current  sampled every `interval` s; `anon` from memory.stat sampled too: the sampled maximum of
                  anon (`peak_anon_bytes`) is the comparable "process memory" figure
  cpu.stat        usage_usec, for the idle check
Elsewhere (macOS + colima, cgroup v1 hosts) the fallback is `docker stats --no-stream` (memory as the
Docker CLI reports it, which excludes inactive file cache) and `docker exec ... cat /sys/fs/cgroup/...`.

ankaad (a host process): VmHWM / VmRSS from /proc/<pid>/status (peak and current resident set,
`peak_rss_bytes` is VmHWM), CPU from /proc/<pid>/stat; on macOS `ps -o rss=,cputime=` (sampled peak).

Disk: `du -sk` of the data directories (allocated blocks), inside the container for containers.
"""
from __future__ import annotations

import os
import platform
import re
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

_CLK_TCK = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100


def _read(path: Path) -> str | None:
    try:
        return path.read_text()
    except OSError:
        return None


def _parse_size(text: str) -> int:
    """`1.5GiB` / `512MiB` / `10kB` (docker stats) -> bytes."""
    m = re.match(r"\s*([0-9.]+)\s*([KMGT]?i?B)", text)
    if not m:
        return 0
    mult = {"B": 1, "kB": 1e3, "KB": 1e3, "KiB": 1024, "MB": 1e6, "MiB": 1024**2, "GB": 1e9, "GiB": 1024**3,
            "TB": 1e12, "TiB": 1024**4}.get(m.group(2), 1)
    return int(float(m.group(1)) * mult)


class Probe:
    """Base: current memory (bytes), CPU seconds consumed so far, peak memory, disk bytes."""

    kind = "base"

    def memory(self) -> dict[str, int]:
        return {}

    def cpu_seconds(self) -> float | None:
        return None

    def peak(self) -> dict[str, int]:
        return {}

    def disk_bytes(self) -> dict[str, int]:
        return {}


class ContainerProbe(Probe):
    kind = "container"

    def __init__(self, container: str, data_paths: list[str]):
        self.container = container
        self.data_paths = data_paths
        cid = subprocess.run(["docker", "inspect", "-f", "{{.Id}}", container], capture_output=True, text=True).stdout.strip()
        if not cid:
            raise RuntimeError(f"container {container} not found")
        self.cid = cid
        self.cgroup = next((p for p in (Path(f"/sys/fs/cgroup/system.slice/docker-{cid}.scope"), Path(f"/sys/fs/cgroup/docker/{cid}"))
                            if (p / "memory.current").exists()), None)
        self.source = "host cgroup v2" if self.cgroup else "docker stats / docker exec"

    def _cg(self, name: str) -> str | None:
        if self.cgroup is not None:
            return _read(self.cgroup / name)
        r = subprocess.run(["docker", "exec", self.container, "cat", f"/sys/fs/cgroup/{name}"], capture_output=True, text=True)
        return r.stdout if r.returncode == 0 else None

    def memory(self) -> dict[str, int]:
        if self.cgroup is not None:
            out = {"current_bytes": int((_read(self.cgroup / "memory.current") or "0").strip() or 0)}
            stat = _read(self.cgroup / "memory.stat") or ""
            for line in stat.splitlines():
                k, _, v = line.partition(" ")
                if k in ("anon", "file"):
                    out[f"{k}_bytes"] = int(v)
            return out
        r = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", self.container], capture_output=True, text=True)
        used = r.stdout.split("/")[0] if r.returncode == 0 else ""
        return {"current_bytes": _parse_size(used)} if used else {}

    def cpu_seconds(self) -> float | None:
        stat = self._cg("cpu.stat")
        if not stat:
            return None
        for line in stat.splitlines():
            if line.startswith("usage_usec"):
                return int(line.split()[1]) / 1e6
        return None

    def peak(self) -> dict[str, int]:
        v = self._cg("memory.peak")
        return {"cgroup_peak_bytes": int(v.strip())} if v and v.strip().isdigit() else {}

    def disk_bytes(self) -> dict[str, int]:
        out = {}
        for p in self.data_paths:
            r = subprocess.run(["docker", "exec", self.container, "du", "-sk", p], capture_output=True, text=True)
            if r.returncode == 0 and r.stdout.split():
                out[p] = int(r.stdout.split()[0]) * 1024
        return out


class ProcessProbe(Probe):
    kind = "process"

    def __init__(self, pid: int, data_paths: list[Path]):
        self.pid = pid
        self.data_paths = data_paths
        self.proc = Path(f"/proc/{pid}")
        self.source = "/proc" if self.proc.exists() else "ps"

    def _status(self) -> dict[str, int]:
        text = _read(self.proc / "status") or ""
        out = {}
        for line in text.splitlines():
            k, _, v = line.partition(":")
            if k in ("VmHWM", "VmRSS"):
                out[k] = int(v.split()[0]) * 1024
        return out

    def memory(self) -> dict[str, int]:
        if self.source == "/proc":
            st = self._status()
            return {"rss_bytes": st["VmRSS"]} if "VmRSS" in st else {}
        r = subprocess.run(["ps", "-o", "rss=", "-p", str(self.pid)], capture_output=True, text=True)
        return {"rss_bytes": int(r.stdout.strip()) * 1024} if r.stdout.strip() else {}

    def cpu_seconds(self) -> float | None:
        if self.source == "/proc":
            stat = _read(self.proc / "stat")
            if not stat:
                return None
            fields = stat.rsplit(")", 1)[1].split()
            return (int(fields[11]) + int(fields[12])) / _CLK_TCK  # utime, stime
        r = subprocess.run(["ps", "-o", "cputime=", "-p", str(self.pid)], capture_output=True, text=True)
        t = r.stdout.strip()
        if not t:
            return None
        parts = [float(x) for x in re.split("[:-]", t)]
        secs = 0.0
        for x in parts:
            secs = secs * 60 + x
        return secs

    def peak(self) -> dict[str, int]:
        st = self._status() if self.source == "/proc" else {}
        return {"peak_rss_bytes": st["VmHWM"]} if "VmHWM" in st else {}

    def disk_bytes(self) -> dict[str, int]:
        out = {}
        for p in self.data_paths:
            if p.exists():
                r = subprocess.run(["du", "-sk", str(p)], capture_output=True, text=True)
                if r.returncode == 0 and r.stdout.split():
                    out[str(p)] = int(r.stdout.split()[0]) * 1024
        return out


@dataclass
class Sampler:
    """Sample a probe's memory every `interval` seconds in a thread; keep the maximum per metric."""

    probe: Probe
    interval: float = 0.5
    maxima: dict[str, int] = field(default_factory=dict)
    samples: int = 0
    _stop: threading.Event = field(default_factory=threading.Event)
    _thread: threading.Thread | None = None

    def __enter__(self) -> "Sampler":
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                for k, v in self.probe.memory().items():
                    self.maxima[k] = max(self.maxima.get(k, 0), v)
                self.samples += 1
            except Exception:  # noqa: BLE001 - a missed sample is not fatal
                pass
            self._stop.wait(self.interval)

    def __exit__(self, *exc) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def result(self) -> dict:
        out = {f"sampled_max_{k}": v for k, v in self.maxima.items()}
        out.update(self.probe.peak())
        out["samples"] = self.samples
        return out


def wait_idle(probe: Probe, max_s: float, cpu_pct: float = 5.0, window_s: float = 5.0) -> dict:
    """Wait until the engine uses less than `cpu_pct` % of ONE core over `window_s` seconds (background
    work after a load: WAL apply, snapshots, tiering, autovacuum), at most `max_s`. Returns what happened."""
    t0 = time.monotonic()
    last = probe.cpu_seconds()
    if last is None:
        return {"settle_s": 0.0, "idle": None, "note": "no CPU counter for this engine on this host"}
    while True:
        time.sleep(window_s)
        now = probe.cpu_seconds()
        busy = 100.0 * ((now or 0.0) - last) / window_s
        last = now
        waited = time.monotonic() - t0
        if busy < cpu_pct:
            return {"settle_s": round(waited, 1), "idle": True, "last_window_cpu_pct": round(busy, 1)}
        if waited >= max_s:
            return {"settle_s": round(waited, 1), "idle": False, "last_window_cpu_pct": round(busy, 1)}


def total_memory_bytes() -> int:
    if platform.system() == "Linux":
        for line in (_read(Path("/proc/meminfo")) or "").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    if platform.system() == "Darwin":
        r = subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True)
        if r.stdout.strip().isdigit():
            return int(r.stdout.strip())
    return 0
