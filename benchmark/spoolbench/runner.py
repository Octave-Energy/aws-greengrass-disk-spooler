"""
Runs one benchmark scenario, black box, the way the system runs in the field:

    IBEMS simulator --IPC--> Nucleus + disk spooler plugin --MQTT/mTLS--> [network proxy] --> broker --> cloud

Phases (each one is recorded in the time series and summarised separately):

1. startup     the Nucleus starts and sets up the persistent spooler
2. connect     the simulator publishes at the scenario rate until the first message reaches the cloud
3. warmup      steady state, not measured
4. online      steady state: ingest rate, IPC latency, CPU (checkpoint: live heap with an empty spool)
5. outage      the internet breaks (proxy black-holes the connection); IBEMS keeps publishing, the spooler
               takes over: first memory (up to diskSpillThresholdBytes), then disk
6. device_crash          optional: during the internet outage the whole device goes down: the Nucleus, the spooler
                        and IBEMS are SIGKILLed at once, without any shutdown, and the device boots again
7. offline_after_reboot  optional: the internet is still down after the reboot; IBEMS publishes again
               (checkpoint at the end of the outage: live heap and spooled messages in memory, db size)
8. recovery    the link is back; IBEMS keeps publishing; until every message accepted before the link came back
               reached the cloud (drained), progress stalls, or max_drain_s
9. cooldown    publishing stops; until every accepted message reached the cloud, or no progress
               (checkpoint: heap after the drain); then graceful stop and final database size

Loss accounting is exact: the simulator records which sequence numbers the Nucleus accepted, the cloud which ones
it received.
"""

import copy
import csv
import json
import shlex
import shutil
import sys
import tempfile
import threading
import time
import traceback
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import certs as certs_module
from .config import Scenario, Settings
from . import nucleus as nucleus_module
from .broker import Broker
from .cloud import Cloud
from .netproxy import NetProxy
from .probe import Probe
from .stats import histogram_percentiles_ms, merge_histograms

BENCHMARK_DIR = Path(__file__).resolve().parent.parent
TOPIC = f"ibems/{nucleus_module.THING_NAME}/measurements/protobuf"

# log events counted in every run (substring match on any Nucleus log line)
LOG_PATTERNS = {
    "disk_add_failed_fallback_to_memory": "Disk Spooler failed to add Message",
    "spool_full_rejected": "Message spool is full",
    "message_larger_than_spool": "Message is larger than the size of message spool",
    "spool_queue_add_failed": "Fail to add publish request to spooler queue",
    "publish_retry": "Failed to publish the message via Spooler and will retry",
    "publish_dropped_after_retries": "will drop the message",
    "spooler_publish_error": "Error when publishing from spooler",
    "db_corrupted_recreated": "is corrupted, creating new database",
    "db_read_failed": "Failed to retrieve message by messageId",
    "db_delete_failed": "Failed to delete message by messageId",
}


class ScenarioRun:
    def __init__(self, scenario: Scenario, settings: Settings, plugin_jar: Path, label: str, out_dir: Path,
                 distribution: Path, keep_workdir: bool = False) -> None:
        self.scenario = scenario
        self.settings = settings
        self.plugin_jar = plugin_jar
        self.label = label
        self.out_dir = out_dir
        self.distribution = distribution
        self.keep_workdir = keep_workdir
        # short path: the Nucleus IPC socket lives under its root and socket paths are limited to ~100 characters
        self.work = Path(tempfile.mkdtemp(prefix="sb-", dir="/tmp"))
        self.simdir = self.work / "sim"
        self.rows: List[Dict[str, Any]] = []
        self.lock = threading.Lock()
        self.phase = "startup"
        self.rate = 0.0
        self.snapshot_id = 0
        self.ipc_hist: Dict[str, Dict[int, int]] = {}
        self.e2e_hist: Dict[str, Dict[int, int]] = {}
        self.checkpoints: Dict[str, Dict[str, Any]] = {}
        self.events: Dict[str, Any] = {}
        self._stats_offset = 0
        self._sim_latest: Dict[str, Any] = {}
        self._stop_sampler = threading.Event()
        self.t0 = time.monotonic()
        self.broker = self.proxy = self.cloud = self.nucleus = self.probe = None

    # --- plumbing ---------------------------------------------------------------------------------------------

    def _control(self, **values) -> None:
        current = {"rate": self.rate, "snapshot": self.snapshot_id}
        current.update(values)
        self.rate = current["rate"]
        tmp = self.simdir / "control.json.tmp"
        tmp.write_text(json.dumps(current))
        tmp.replace(self.simdir / "control.json")

    def _snapshot(self, rate: float, timeout_s: float = 90) -> Dict[str, Any]:
        """Exact simulator counters and accepted sequence numbers (in-flight publishes completed first)."""
        self.snapshot_id += 1
        self._control(rate=rate, snapshot=self.snapshot_id)
        path = self.simdir / f"snapshot-{self.snapshot_id}.json"
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if path.exists():
                snapshot = json.loads(path.read_text())
                snapshot["bits"] = (self.simdir / "accepted.bin").read_bytes()
                return snapshot
            time.sleep(0.05)
        raise TimeoutError(f"simulator did not write {path.name} within {timeout_s}s")

    def _read_sim_stats(self) -> None:
        path = self.simdir / "stats.jsonl"
        if not path.exists():
            return
        with open(path, "rb") as f:
            f.seek(self._stats_offset)
            data = f.read()
        end = data.rfind(b"\n")
        if end < 0:
            return
        self._stats_offset += end + 1
        for line in data[: end + 1].splitlines():
            try:
                stats = json.loads(line)
            except ValueError:
                continue
            merge_histograms(self.ipc_hist.setdefault(self.phase, {}), stats.pop("latency_hist", {}))
            self._sim_latest = stats

    def _sample(self) -> Dict[str, Any]:
        with self.lock:
            self._read_sim_stats()
            sim = self._sim_latest
            cloud = self.cloud.counters()
            merge_histograms(self.e2e_hist.setdefault(self.phase, {}), self.cloud.take_latency_histogram())
            row: Dict[str, Any] = {"t": round(time.monotonic() - self.t0, 3), "phase": self.phase,
                                   "target_rate": self.rate, "link": self.proxy.mode}
            for key in ("published", "accepted", "rejected", "accepted_bytes", "outstanding", "lag_s"):
                row[key] = sim.get(key, 0)
            row["sim_time"] = sim.get("time")  # when the simulator reported those counters (once a second)
            row["delivered"] = cloud["unique"]
            row["duplicates"] = cloud["duplicates"]
            row["delivered_bytes"] = cloud["bytes"]
            row["backlog"] = max(row["accepted"] - row["delivered"], 0)
            row.update(self.probe.sample())
            row["proxy_bytes_up"] = self.proxy.bytes_up
            self.rows.append(row)
            return row

    def _sampler(self) -> None:
        while not self._stop_sampler.wait(1.0):
            try:
                self._sample()
            except Exception:  # noqa: BLE001 - a missed sample must not abort the run
                traceback.print_exc()

    def _set_phase(self, phase: str) -> None:
        """Closes the current phase with a sample, then starts the next one."""
        self._sample()
        with self.lock:
            self.phase = phase
        self._log(f"phase {phase}")

    def _checkpoint(self, name: str) -> None:
        started = time.monotonic()
        self.checkpoints[name] = self.probe.checkpoint()
        self._log(f"checkpoint {name}: live heap {self.checkpoints[name].get('live_heap_bytes')} B, "
                  f"spool messages {self.checkpoints[name].get('spool_message_instances')}, "
                  f"db {self.checkpoints[name].get('spool_db_bytes')} B ({time.monotonic() - started:.1f}s)")

    def _log(self, message: str) -> None:
        print(f"[{self.label} {self.scenario.name} {time.monotonic() - self.t0:7.1f}s] {message}", flush=True)

    def _sleep_phase(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if self.nucleus.process.poll() is not None:
                raise RuntimeError(f"Nucleus exited with {self.nucleus.process.returncode}")
            time.sleep(min(0.5, max(deadline - time.monotonic(), 0)))

    def _wait_delivered(self, snapshot: Dict[str, Any], max_s: float, stall_s: float) -> Dict[str, Any]:
        """Waits until every message accepted before `snapshot` reached the cloud; measures how long it took."""
        bits, below = snapshot["bits"], snapshot["next_seq"]
        started = time.monotonic()
        unique_at_start = self.cloud.counters()["unique"]
        backlog = len(self.cloud.missing(bits, below))
        best, last_progress, first_delivery_s = backlog, started, None
        result = {"backlog_msgs": backlog, "drained": backlog == 0, "drain_s": 0.0 if backlog == 0 else None,
                  "stalled": False, "remaining_msgs": backlog}
        next_check = started
        while backlog > 0:
            time.sleep(0.1)
            now = time.monotonic()
            if first_delivery_s is None and self.cloud.counters()["unique"] > unique_at_start:
                first_delivery_s = now - started
            if now < next_check:
                continue
            next_check = now + 1.0
            remaining = len(self.cloud.missing(bits, below))
            result["remaining_msgs"] = remaining
            if remaining == 0:
                result.update(drained=True, drain_s=round(now - started, 3))
                last_progress = now
                break
            if remaining < best:
                best, last_progress = remaining, now
            if now - last_progress > stall_s:
                result["stalled"] = True
                break
            if now - started > max_s:
                break
            if self.nucleus.process.poll() is not None:
                raise RuntimeError(f"Nucleus exited with {self.nucleus.process.returncode}")
        result["first_delivery_s"] = round(first_delivery_s, 3) if first_delivery_s is not None else None
        # rate at which the backlog went down, up to the last progress (a stall at the end does not count)
        progress_s = last_progress - started
        delivered = backlog - (0 if result["drained"] else best)
        result["delivery_rate_msg_s"] = round(delivered / progress_s, 1) if progress_s > 0 and delivered else None
        return result

    # --- scenario -----------------------------------------------------------------------------------------------

    def run(self) -> Dict[str, Any]:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        result: Dict[str, Any] = {"label": self.label, "scenario": asdict(self.scenario), "status": "ok"}
        sampler = threading.Thread(target=self._sampler, daemon=True)
        try:
            self._setup()
            sampler.start()
            self._run_phases()
        except Exception as e:  # noqa: BLE001 - reported in the result, the benchmark continues
            traceback.print_exc()
            result["status"] = "failed"
            result["error"] = f"{type(e).__name__}: {e}"
        finally:
            self._stop_sampler.set()
            if sampler.is_alive():
                sampler.join(5)
            self._teardown()
        result.update(self._results())
        self._write(result)
        return result

    def _setup(self) -> None:
        s = self.settings
        certs = certs_module.create(self.work / "certs")
        self.broker = Broker(self.work / "broker", certs, **({"image": s.broker_image} if s.broker_image else {}))
        self.broker.start()
        self.proxy = NetProxy(self.broker.tls_port)
        self.proxy.start()
        self.cloud = Cloud(self.broker.plain_port, TOPIC)
        self.cloud.start()

        self.simdir.mkdir()
        self._control(rate=0)
        command = " ".join(["exec", shlex.quote(sys.executable), "-u", "-m", "spoolbench.simulator",
                            "--workdir", shlex.quote(str(self.simdir)), "--topic", shlex.quote(TOPIC),
                            "--seed", str(s.seed), "--max-outstanding", str(s.max_outstanding),
                            "--profile", shlex.quote(json.dumps(s.payload_profile))])
        plugin_configuration = copy.deepcopy(s.plugin_configuration)
        plugin_configuration["diskSpillThresholdBytes"] = self.scenario.spill_threshold_bytes
        self.nucleus = nucleus_module.Nucleus(self.work / "gg", self.distribution, s.java_home, s.jvm_options,
                                              self.out_dir / "logs")
        self.nucleus.install(self.plugin_jar, certs, self.proxy.listen_port, s.nucleus_configuration,
                             plugin_configuration, command, {"PYTHONPATH": str(BENCHMARK_DIR)})
        self.probe = Probe(self.nucleus.spooler_db, s.java_home)
        self.t0 = time.monotonic()

    def _start_nucleus(self) -> float:
        started = time.monotonic()
        offset = self.nucleus.log_size()
        pid = self.nucleus.start()
        self.nucleus.wait_until_spooler_ready(self.settings.startup_timeout_s, offset)
        self.probe.attach(pid)
        return round(time.monotonic() - started, 3)

    def _run_phases(self) -> None:
        sc = self.scenario
        self.events["startup_s"] = self._start_nucleus()
        self._log(f"spooler ready after {self.events['startup_s']}s")

        self._set_phase("connect")
        self._control(rate=sc.rate)
        deadline = time.monotonic() + self.settings.connect_timeout_s
        while self.cloud.counters()["unique"] == 0:
            if time.monotonic() > deadline:
                raise TimeoutError("no message reached the cloud: the Nucleus did not connect")
            self._sleep_phase(0.2)
        self.events["connect_s"] = round(time.monotonic() - self.t0 - self.events["startup_s"], 3)

        self._set_phase("warmup")
        self._sleep_phase(sc.warmup_s)
        self._set_phase("online")
        self._sleep_phase(sc.online_s)
        self._checkpoint("online_end")

        self._set_phase("outage")
        self.proxy.reset() if sc.outage_mode == "reset" else self.proxy.blackhole()
        self._sleep_phase(sc.outage_s)
        if sc.device_crash:
            before = self._snapshot(rate=0)  # IBEMS dies with the device: the exact set accepted before the crash
            self.events["accepted_before_crash"] = before["accepted"]
            self._set_phase("device_crash")
            self.probe.detach()
            self.nucleus.kill()
            # what the dead device had sent but the link had not carried yet dies with it: drop the connections
            # (and the bytes the proxy holds for them), then keep the link down
            self.proxy.reset()
            if sc.outage_mode != "reset":
                self.proxy.blackhole()
            self.events["reboot_s"] = self._start_nucleus()
            self._log(f"rebooted after the crash, spooler ready after {self.events['reboot_s']}s")
            self._set_phase("offline_after_reboot")
            self._control(rate=sc.rate)
            self._sleep_phase(sc.offline_after_reboot_s)
        self._checkpoint("outage_end")
        at_link_up = self._snapshot(rate=sc.rate)

        self._set_phase("recovery")
        self.proxy.up()
        self.events["recovery"] = self._wait_delivered(at_link_up, sc.max_drain_s, sc.stall_s)
        self._log(f"recovery: {self.events['recovery']}")

        self._set_phase("cooldown")
        final = self._snapshot(rate=0)
        self.events["cooldown"] = self._wait_delivered(final, sc.max_cooldown_s, sc.cooldown_s)
        self.events["lost_seqs"] = self.cloud.missing(final["bits"], final["next_seq"])[:1000]
        self.events["final"] = {k: final[k] for k in ("next_seq", "published", "accepted", "rejected",
                                                      "accepted_bytes", "errors")}
        self._checkpoint("drained")
        self._set_phase("shutdown")

    def _teardown(self) -> None:
        if self.probe:
            self.probe.detach()
        if self.nucleus and self.nucleus.process:
            try:
                self.nucleus.stop()
            except Exception:  # noqa: BLE001
                traceback.print_exc()
            self.events["db_bytes_after_stop"] = self.probe.spooler_files_bytes() if self.probe else None
            self.nucleus.collect_logs()
            self.events["log"] = self.nucleus.log_summary()
            self.events["log_patterns"] = self._count_log_patterns()
        for component in (self.cloud, self.proxy, self.broker):
            if component:
                try:
                    component.stop()
                except Exception:  # noqa: BLE001
                    traceback.print_exc()
        (self.out_dir / "logs").mkdir(parents=True, exist_ok=True)
        if self.broker and (self.broker.workdir / "mosquitto.log").exists():
            shutil.copyfile(self.broker.workdir / "mosquitto.log", self.out_dir / "logs" / "mosquitto.log")
        for name in ("stats.jsonl",):
            if (self.simdir / name).exists():
                shutil.copyfile(self.simdir / name, self.out_dir / "logs" / f"simulator-{name}")
        if not self.keep_workdir:
            shutil.rmtree(self.work, ignore_errors=True)

    def _count_log_patterns(self) -> Dict[str, int]:
        counts = {key: 0 for key in LOG_PATTERNS}
        logs = self.nucleus.root / "logs"
        for log in (sorted(logs.glob("*.log")) if logs.exists() else []):
            with open(log, errors="replace") as f:
                for line in f:
                    for key, pattern in LOG_PATTERNS.items():
                        if pattern in line:
                            counts[key] += 1
        return counts

    # --- results ------------------------------------------------------------------------------------------------

    def _phase_metrics(self) -> Dict[str, Dict[str, Any]]:
        phases: Dict[str, Dict[str, Any]] = {}
        previous: Optional[Dict[str, Any]] = None

        def last(rows, key):
            values = [r[key] for r in rows if r.get(key) is not None]
            return values[-1] if values else None

        order: List[str] = []
        for row in self.rows:
            if row["phase"] not in order:
                order.append(row["phase"])
        for phase in order:
            rows = [r for r in self.rows if r["phase"] == phase]
            start = previous or rows[0]
            end = rows[-1]
            duration = end["t"] - start["t"]

            def delta(key):
                a, b = last([start], key), last(rows, key)
                return (b - a) if a is not None and b is not None else None

            metrics: Dict[str, Any] = {"duration_s": round(duration, 3)}
            for key in ("published", "accepted", "rejected", "accepted_bytes", "delivered", "duplicates",
                        "delivered_bytes", "lag_s", "cpu_s", "read_bytes", "write_bytes"):
                metrics[key] = delta(key)
            # the simulator counters are up to a second old: divide them by the simulator's own interval
            sim_duration = delta("sim_time")
            if sim_duration and sim_duration > 0:
                metrics["accepted_rate"] = round((metrics["accepted"] or 0) / sim_duration, 2)
            if duration > 0:
                metrics.setdefault("accepted_rate", round((metrics["accepted"] or 0) / duration, 2))
                metrics["delivered_rate"] = round((metrics["delivered"] or 0) / duration, 2)
                if metrics["cpu_s"] is not None:
                    metrics["cpu_pct"] = round(metrics["cpu_s"] / duration * 100, 1)
            for key in ("rss_bytes", "heap_used_bytes", "spool_db_bytes", "backlog"):
                values = [r[key] for r in rows if r.get(key) is not None]
                metrics[f"{key}_max"] = max(values) if values else None
            metrics["spool_db_bytes_start"] = last([start], "spool_db_bytes")
            metrics["spool_db_bytes_end"] = last(rows, "spool_db_bytes")
            metrics["rss_bytes_start"] = last([start], "rss_bytes")
            metrics["rss_bytes_end"] = last(rows, "rss_bytes")
            metrics["backlog_end"] = end.get("backlog")
            metrics["ipc_latency_ms"] = histogram_percentiles_ms(self.ipc_hist.get(phase, {}))
            metrics["e2e_latency_ms"] = histogram_percentiles_ms(self.e2e_hist.get(phase, {}))
            phases[phase] = metrics
            previous = end
        return phases

    def _summary(self, phases: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """The headline metrics compared between runs (see compare.METRICS)."""
        sc = self.scenario
        online = phases.get("online", {})
        outage_phases = [phases[p] for p in ("outage", "offline_after_reboot") if p in phases]
        checkpoints = self.checkpoints
        recovery = self.events.get("recovery", {})
        cooldown = self.events.get("cooldown", {})
        final = self.events.get("final", {})
        cp_online, cp_outage, cp_drained = (checkpoints.get(k, {}) for k in ("online_end", "outage_end", "drained"))

        def total(key):
            values = [p.get(key) for p in outage_phases if p.get(key) is not None]
            return sum(values) if values else None

        def sub(a, b):
            return a - b if a is not None and b is not None else None

        def ratio(a, b):
            return round(a / b, 3) if a is not None and b else None

        outage_accepted_bytes = total("accepted_bytes")
        db_growth = sub(cp_outage.get("spool_db_bytes"), cp_online.get("spool_db_bytes"))
        write_bytes_outage = total("write_bytes")
        rows_db = [r["spool_db_bytes"] for r in self.rows if r.get("spool_db_bytes") is not None]
        rows_rss = [r["rss_bytes"] for r in self.rows if r.get("rss_bytes") is not None]
        rows_heap = [r["heap_used_bytes"] for r in self.rows if r.get("heap_used_bytes") is not None]
        patterns = self.events.get("log_patterns", {})
        log = self.events.get("log", {})
        summary = {
            # ingest: can IBEMS publish at the rate it needs, and how fast does the Nucleus accept?
            "ingest_rate_msg_s": online.get("accepted_rate"),
            "ingest_shortfall_pct": round((1 - online["accepted_rate"] / sc.rate) * 100, 2)
            if online.get("accepted_rate") is not None else None,
            "ipc_latency_p50_ms": online.get("ipc_latency_ms", {}).get("p50"),
            "ipc_latency_p99_ms": online.get("ipc_latency_ms", {}).get("p99"),
            "e2e_latency_p50_ms": online.get("e2e_latency_ms", {}).get("p50"),
            "e2e_latency_p99_ms": online.get("e2e_latency_ms", {}).get("p99"),
            "cpu_pct_online": online.get("cpu_pct"),
            # delivery: does the Nucleus get messages to the cloud as fast as IBEMS produces them?
            "delivery_rate_online_msg_s": online.get("delivered_rate"),
            "backlog_online_end_msgs": online.get("backlog_end"),
            # outage: the spooler takes over
            "outage_accepted_msgs": total("accepted"),
            "outage_accepted_payload_bytes": outage_accepted_bytes,
            "outage_rejected_msgs": total("rejected"),
            "ipc_latency_p99_ms_outage": phases.get("outage", {}).get("ipc_latency_ms", {}).get("p99"),
            "cpu_pct_outage": phases.get("outage", {}).get("cpu_pct"),
            # disk
            "disk_db_bytes_peak": max(rows_db) if rows_db else None,
            "disk_db_growth_outage_bytes": db_growth,
            "disk_bytes_per_payload_byte": ratio(db_growth, outage_accepted_bytes),
            "disk_db_bytes_after_stop": self.events.get("db_bytes_after_stop"),
            "disk_write_bytes_outage": write_bytes_outage,
            "disk_write_amplification": ratio(write_bytes_outage, outage_accepted_bytes),
            # memory
            "heap_live_online_bytes": cp_online.get("live_heap_bytes"),
            "heap_live_outage_end_bytes": cp_outage.get("live_heap_bytes"),
            # not after a device crash: the checkpoints are then of two different JVMs
            "heap_backlog_bytes": None if sc.device_crash else sub(cp_outage.get("live_heap_bytes"),
                                                                 cp_online.get("live_heap_bytes")),
            "heap_live_drained_bytes": cp_drained.get("live_heap_bytes"),
            "spool_messages_in_heap_outage_end": cp_outage.get("spool_message_instances"),
            "heap_used_peak_bytes": max(rows_heap) if rows_heap else None,
            "rss_peak_bytes": max(rows_rss) if rows_rss else None,
            "rss_growth_outage_bytes": None if sc.device_crash else sub(cp_outage.get("rss_bytes"),
                                                                      cp_online.get("rss_bytes")),
            # recovery
            "recovery_backlog_msgs": recovery.get("backlog_msgs"),
            "recovery_first_delivery_s": recovery.get("first_delivery_s"),
            "recovery_drain_s": recovery.get("drain_s"),
            "recovery_drain_rate_msg_s": round(recovery["backlog_msgs"] / recovery["drain_s"], 1)
            if recovery.get("drain_s") else None,
            "recovery_drained": recovery.get("drained"),
            "recovery_delivery_rate_msg_s": phases.get("recovery", {}).get("delivered_rate"),
            "backlog_recovery_end_msgs": phases.get("recovery", {}).get("backlog_end"),
            # with no new messages and a backlog to work through, the delivery rate is the spooler's capacity
            "cooldown_delivery_rate_msg_s": cooldown.get("delivery_rate_msg_s")
            if cooldown.get("backlog_msgs", 0) >= 100 else None,
            "cpu_pct_recovery": phases.get("recovery", {}).get("cpu_pct"),
            # integrity
            # accepted by the Nucleus (IPC publish succeeded) but never delivered to the cloud
            "lost_msgs": cooldown.get("remaining_msgs") if cooldown.get("drained") or cooldown.get("stalled")
            else None,
            # still being delivered when the benchmark gave up waiting (max_cooldown_s)
            "undelivered_msgs": cooldown.get("remaining_msgs") if not (cooldown.get("drained") or
                                                                         cooldown.get("stalled")) else 0,
            "duplicate_msgs": self.cloud.counters()["duplicates"] if self.cloud else None,
            "out_of_order_msgs": self.cloud.counters()["out_of_order"] if self.cloud else None,
            "rejected_msgs": final.get("rejected"),
            "accepted_msgs": final.get("accepted"),
            # robustness
            "startup_s": self.events.get("startup_s"),
            "reboot_s": self.events.get("reboot_s"),
            "log_errors": log.get("error"),
            "log_warnings": log.get("warn"),
            **{f"log_{key}": value for key, value in patterns.items()},
        }
        return summary

    def _results(self) -> Dict[str, Any]:
        phases = self._phase_metrics() if self.rows else {}
        events = {k: v for k, v in self.events.items() if k not in ("log",)}
        return {"phases": phases, "checkpoints": self.checkpoints, "events": events,
                "log": self.events.get("log"), "summary": self._summary(phases)}

    def _write(self, result: Dict[str, Any]) -> None:
        keys: List[str] = []
        for row in self.rows:
            keys.extend(k for k in row if k not in keys)
        with open(self.out_dir / "timeseries.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(self.rows)
        (self.out_dir / "result.json").write_text(json.dumps(result, indent=2, default=str))
