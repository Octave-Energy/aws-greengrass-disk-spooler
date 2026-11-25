"""Describes the machine a benchmark ran on, so that only comparable reports are compared."""

import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import psutil


def _cpu_model() -> str:
    try:
        if sys.platform == "darwin":
            return subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True,
                                  text=True).stdout.strip()
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor()


def _java_version(java_home: Optional[str]) -> str:
    java = str(Path(java_home) / "bin" / "java") if java_home else "java"
    try:
        out = subprocess.run([java, "-version"], capture_output=True, text=True).stderr
        return out.strip().splitlines()[0] if out.strip() else "unknown"
    except OSError:
        return "unknown"


def collect(java_home: Optional[str], nucleus_version: str) -> Dict[str, Any]:
    disk = psutil.disk_usage("/tmp")
    return {
        "os": f"{platform.system()} {platform.release()}",
        "machine": platform.machine(),
        "cpu": _cpu_model(),
        "cpus": psutil.cpu_count(),
        "memory_bytes": psutil.virtual_memory().total,
        "tmp_disk_free_bytes": disk.free,
        "python": platform.python_version(),
        "java": _java_version(java_home),
        "nucleus": nucleus_version,
        "github_run": f"{os.environ.get('GITHUB_SERVER_URL', '')}/{os.environ.get('GITHUB_REPOSITORY', '')}"
                      f"/actions/runs/{os.environ['GITHUB_RUN_ID']}" if os.environ.get("GITHUB_RUN_ID") else None,
        "runner": os.environ.get("RUNNER_NAME"),
    }
