"""Hardware / OS / toolchain facts recorded with every measurement (docs/measurement-protocol.md)."""
from __future__ import annotations

import platform
import shutil
import socket
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from . import REPO_DIR


def _run(cmd: list[str], timeout: float = 10) -> str | None:
    if not shutil.which(cmd[0]):
        return None
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout).stdout.strip() or None
    except Exception:  # noqa: BLE001 - best effort
        return None


def _read(path: str) -> str | None:
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def git_commit() -> str | None:
    """Commit the tree was built from: git when this is a checkout, else the `.source-commit` file
    that `scripts/gcp/ankaa-gcp.sh deploy` writes next to an exported tree."""
    commit = _run(["git", "-C", str(REPO_DIR), "rev-parse", "HEAD"])
    if commit:
        return commit
    try:
        return (REPO_DIR / ".source-commit").read_text().strip() or None
    except OSError:
        return None


def is_arm_server_linux() -> bool:
    """True only on aarch64 Linux (the only platform whose numbers may be published)."""
    return platform.system() == "Linux" and platform.machine() in ("aarch64", "arm64")


def host_label() -> str:
    """Label for result files and reports.

    Result files are meant to be published: the machine's real hostname is not written unless
    the operator asks for a label with ANKAA_BENCH_HOST_LABEL (e.g. "c8g.4xlarge-run1").
    """
    import os

    label = os.environ.get("ANKAA_BENCH_HOST_LABEL", "").strip()
    if label:
        return "".join(c if c.isalnum() or c in "-_." else "-" for c in label)[:64]
    return f"{platform.machine()}-{platform.system().lower()}"


def collect(ankaad: Path | None = None) -> dict:
    info: dict = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "hostname": host_label(),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "git_commit": git_commit(),
        "publishable_platform": is_arm_server_linux(),
    }
    if platform.system() == "Linux":
        info["lscpu"] = _run(["lscpu"])
        info["numactl"] = _run(["numactl", "-H"])
        info["kernel_cmdline"] = _read("/proc/cmdline")
        mem = _read("/proc/meminfo") or ""
        info["meminfo_total"] = next((ln for ln in mem.splitlines() if ln.startswith("MemTotal")), None)
        info["os_release"] = _read("/etc/os-release")
        info["dmi_product_name"] = _read("/sys/devices/virtual/dmi/id/product_name")
    elif platform.system() == "Darwin":
        info["cpu_brand"] = _run(["sysctl", "-n", "machdep.cpu.brand_string"])
        info["mem_bytes"] = _run(["sysctl", "-n", "hw.memsize"])
        info["note"] = "macOS laptop: smoke test only, never a publishable measurement"
    info["rustc"] = _run(["rustc", "--version"])
    if ankaad is not None:
        info["ankaad_version"] = _run([str(ankaad), "--version"])
    return info
