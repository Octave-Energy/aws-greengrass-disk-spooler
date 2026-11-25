"""
Observes the Nucleus from the outside, like an operator would on a device:

- every second: process RSS, CPU time, bytes read/written to storage (Linux), threads, and the size of the spooler
  database files; JVM heap use and GC counts from a streaming `jstat`;
- at checkpoints: `jcmd GC.class_histogram`, which forces a full GC and reports the live heap, and how many spool
  messages (and their payload byte arrays) the JVM holds.
"""

import re
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import psutil

SPOOLER_FILE_SUFFIXES = ["", "-journal", "-wal", "-shm"]
HISTOGRAM_CLASSES = {
    "spool_message": "com.aws.greengrass.mqttclient.spool.SpoolMessage",
    "publish": "com.aws.greengrass.mqttclient.v5.Publish",
    "byte_array": "[B",
}


class JstatStream:
    """`jstat -gc <pid> 1000`: heap used/committed and GC counts, parsed by column name (portable across JDKs)."""

    def __init__(self, java_home: Optional[str], pid: int) -> None:
        jstat = str(Path(java_home) / "bin" / "jstat") if java_home else "jstat"
        self.latest: Dict[str, float] = {}
        # the JVM formats numbers in the system locale (e.g. "24576,0"): ask for English
        command = [jstat, "-J-Duser.language=en", "-J-Duser.country=US", "-gc", str(pid), "1000"]
        self.process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self) -> None:
        header: List[str] = []
        for line in self.process.stdout:
            fields = line.split()
            if not fields:
                continue
            if fields[0] == "S0C":
                header = fields
                continue
            if header and len(fields) == len(header):
                try:
                    values = dict(zip(header, (float(f.replace(",", ".")) if f != "-" else 0.0 for f in fields)))
                except ValueError:
                    continue
                used = sum(values.get(k, 0.0) for k in ("S0U", "S1U", "EU", "OU"))
                committed = sum(values.get(k, 0.0) for k in ("S0C", "S1C", "EC", "OC"))
                self.latest = {"heap_used_bytes": used * 1024, "heap_committed_bytes": committed * 1024,
                               "young_gcs": values.get("YGC", 0.0), "full_gcs": values.get("FGC", 0.0),
                               "gc_time_s": values.get("GCT", 0.0)}

    def stop(self) -> None:
        self.process.terminate()


def class_histogram(java_home: Optional[str], pid: int) -> Dict[str, Any]:
    """Live heap after a full GC, and instance counts of the classes that hold spooled messages."""
    jcmd = str(Path(java_home) / "bin" / "jcmd") if java_home else "jcmd"
    started = time.monotonic()
    try:
        out = subprocess.run([jcmd, str(pid), "GC.class_histogram"], capture_output=True, text=True, timeout=120).stdout
    except (subprocess.TimeoutExpired, OSError) as e:
        return {"error": str(e)}
    result: Dict[str, Any] = {"duration_s": round(time.monotonic() - started, 3)}
    for line in out.splitlines():
        fields = line.split()
        if len(fields) >= 3 and fields[0] == "Total":
            result["live_heap_bytes"] = int(fields[2])
            result["live_objects"] = int(fields[1])
        elif len(fields) >= 4 and re.match(r"^\d+:$", fields[0]):
            for key, class_name in HISTOGRAM_CLASSES.items():
                if fields[3] == class_name:
                    result[f"{key}_instances"] = int(fields[1])
                    result[f"{key}_bytes"] = int(fields[2])
    for key in HISTOGRAM_CLASSES:
        result.setdefault(f"{key}_instances", 0)
        result.setdefault(f"{key}_bytes", 0)
    return result


class Probe:
    def __init__(self, spooler_db: Path, java_home: Optional[str]) -> None:
        self.spooler_db = spooler_db
        self.java_home = java_home
        self.process: Optional[psutil.Process] = None
        self.jstat: Optional[JstatStream] = None
        self.io_supported = hasattr(psutil.Process, "io_counters")
        # counters of previous Nucleus processes, so that totals stay cumulative over a restart
        self._carry = {"cpu_s": 0.0, "read_bytes": 0, "write_bytes": 0, "read_syscalls": 0, "write_syscalls": 0}
        self._last = dict(self._carry)

    def attach(self, pid: int) -> None:
        self.detach()
        self._carry = dict(self._last)
        self.process = psutil.Process(pid)
        self.jstat = JstatStream(self.java_home, pid)

    def detach(self) -> None:
        if self.jstat:
            self.jstat.stop()
            self.jstat = None
        self.process = None

    def spooler_files_bytes(self) -> int:
        total = 0
        for suffix in SPOOLER_FILE_SUFFIXES:
            path = self.spooler_db.with_name(self.spooler_db.name + suffix)
            try:
                total += path.stat().st_size
            except OSError:
                pass
        return total

    def sample(self) -> Dict[str, Any]:
        sample: Dict[str, Any] = {"spool_db_bytes": self.spooler_files_bytes()}
        if self.process is None:
            return sample
        try:
            with self.process.oneshot():
                cpu = self.process.cpu_times()
                sample["rss_bytes"] = self.process.memory_info().rss
                sample["threads"] = self.process.num_threads()
                current = {"cpu_s": cpu.user + cpu.system}
                if self.io_supported:
                    io = self.process.io_counters()
                    current.update(read_bytes=io.read_bytes, write_bytes=io.write_bytes,
                                   read_syscalls=io.read_count, write_syscalls=io.write_count)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return sample
        for key, base in self._carry.items():
            if key in current:
                self._last[key] = base + current[key]
                sample[key] = self._last[key]
        if self.jstat and self.jstat.latest:
            sample.update(self.jstat.latest)
        return sample

    def checkpoint(self) -> Dict[str, Any]:
        result = {"spool_db_bytes": self.spooler_files_bytes()}
        if self.process is not None:
            result.update(class_histogram(self.java_home, self.process.pid))
            try:
                result["rss_bytes"] = self.process.memory_info().rss
            except psutil.NoSuchProcess:
                pass
        return result
