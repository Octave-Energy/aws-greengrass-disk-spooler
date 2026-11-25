"""
IBEMS simulator: a Greengrass component publishing IBEMS-like protobuf batches through Greengrass IPC, as IBEMS does
(`publish_to_iot_core_async`, QoS 1, topic `ibems/<thing>/measurements/protobuf`).

It is started and restarted by the Nucleus like any component, and driven by the benchmark through files in its
work directory:

- control.json (read): {"rate": messages per second, "snapshot": n, "stop": bool}
- stats.jsonl (appended every second): published, accepted, rejected, IPC latency histogram, lag
- snapshot-<n>.json (written when "snapshot" changes, after in-flight publishes completed): exact counters
- accepted.bin / state.json: which sequence numbers the Nucleus accepted, and the next sequence number, so that the
  numbering survives a restart (e.g. after a device crash)
"""

import argparse
import json
import math
import os
import signal
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Dict, Optional

from .payloads import PayloadPool, PayloadProfile

LATENCY_BUCKET_BASE = 1.1  # histogram bucket i covers [1.1^i, 1.1^(i+1)) microseconds


def latency_bucket(latency_s: float) -> int:
    return int(math.log(max(latency_s * 1e6, 1.0), LATENCY_BUCKET_BASE))


class Simulator:
    def __init__(self, workdir: Path, topic: str, seed: int, max_outstanding: int, profile: PayloadProfile) -> None:
        self.workdir = workdir
        self.topic = topic
        self.max_outstanding = max_outstanding
        self.pool = PayloadPool(seed=seed, profile=profile)
        self.lock = threading.Condition()
        self.outstanding = 0
        self.rate = 0.0
        self.snapshot_id = 0
        self.stopping = False
        state = self._read_json(workdir / "state.json") or {}
        self.next_seq = state.get("next_seq", 0)
        self.accepted_bits = bytearray(1 << 20)
        if (workdir / "accepted.bin").exists():
            saved = (workdir / "accepted.bin").read_bytes()
            self.accepted_bits[: len(saved)] = saved
        # cumulative counters, carried over a restart through state.json, and per-interval ones
        self.published = state.get("published", 0)
        self.accepted = state.get("accepted", 0)
        self.rejected = state.get("rejected", 0)
        self.accepted_bytes = state.get("accepted_bytes", 0)
        self.errors: Dict[str, int] = dict(state.get("errors", {}))
        self.interval_latencies: Dict[int, int] = {}
        self.lag_s = 0.0
        self.client = None

    # --- IPC -----------------------------------------------------------------------------------------------------

    def connect(self) -> None:
        from awsiot.greengrasscoreipc.clientv2 import GreengrassCoreIPCClientV2

        self.client = GreengrassCoreIPCClientV2()

    def _publish(self, seq: int) -> None:
        from awsiot.greengrasscoreipc.model import QOS

        payload = self.pool.payload(seq, int(time.time() * 1000))
        started = time.monotonic()
        with self.lock:
            self.outstanding += 1
            self.published += 1
        try:
            future = self.client.publish_to_iot_core_async(topic_name=self.topic, qos=QOS.AT_LEAST_ONCE,
                                                           payload=payload)
        except Exception as e:  # noqa: BLE001 - recorded as a rejected publish
            self._done(seq, len(payload), started, e)
            return
        future.add_done_callback(lambda f: self._done(seq, len(payload), started, f.exception()))

    def _done(self, seq: int, size: int, started: float, error: Optional[BaseException]) -> None:
        latency = time.monotonic() - started
        with self.lock:
            self.outstanding -= 1
            bucket = latency_bucket(latency)
            self.interval_latencies[bucket] = self.interval_latencies.get(bucket, 0) + 1
            if error is None:
                self.accepted += 1
                self.accepted_bytes += size
                byte = seq >> 3
                if byte >= len(self.accepted_bits):
                    self.accepted_bits.extend(bytearray(max(byte + 1 - len(self.accepted_bits), 1 << 20)))
                self.accepted_bits[byte] |= 1 << (seq & 7)
            else:
                self.rejected += 1
                key = f"{type(error).__name__}: {str(error)[:100]}"
                self.errors[key] = self.errors.get(key, 0) + 1
            self.lock.notify_all()

    # --- main loop -------------------------------------------------------------------------------------------------

    def run(self) -> None:
        self.connect()
        stats_thread = threading.Thread(target=self._stats_loop, daemon=True)
        stats_thread.start()
        next_send = time.monotonic()
        last_control = 0.0
        while not self.stopping:
            now = time.monotonic()
            if now - last_control >= 0.1:
                last_control = now
                self._read_control()
            if self.rate <= 0:
                time.sleep(0.02)
                next_send = time.monotonic()
                continue
            if now < next_send:
                time.sleep(min(next_send - now, 0.02))
                continue
            with self.lock:
                while self.outstanding >= self.max_outstanding and not self.stopping:
                    self.lock.wait(0.05)
            behind = time.monotonic() - next_send
            if behind > 1.0:
                # more than a second behind schedule: the Nucleus cannot keep up; do not try to catch up the backlog
                self.lag_s += behind
                next_send = time.monotonic()
            self._publish(self.next_seq)
            self.next_seq += 1
            next_send += 1.0 / self.rate
        self._take_snapshot(self.snapshot_id)

    def _read_control(self) -> None:
        control = self._read_json(self.workdir / "control.json")
        if control is None:
            return
        self.rate = float(control.get("rate", 0))
        if control.get("stop"):
            self.stopping = True
        snapshot = int(control.get("snapshot", 0))
        if snapshot != self.snapshot_id:
            self.snapshot_id = snapshot
            self._take_snapshot(snapshot)

    def _take_snapshot(self, snapshot: int) -> None:
        """Waits for in-flight publishes, then persists exact counters and the accepted sequence numbers.

        Never writes a snapshot with publishes still in flight: one accepted later would be missing from the accepted
        set. If a publish never completes, the runner's snapshot timeout fails the scenario instead.
        """
        with self.lock:
            while self.outstanding > 0:
                self.lock.wait(0.05)
            used = (self.next_seq >> 3) + 1
            bits = bytes(self.accepted_bits[:used])
            summary = {"snapshot": snapshot, "next_seq": self.next_seq, "published": self.published,
                       "accepted": self.accepted, "rejected": self.rejected, "accepted_bytes": self.accepted_bytes,
                       "outstanding": self.outstanding, "errors": dict(self.errors), "time": time.time()}
        self._write_atomic(self.workdir / "accepted.bin", bits)
        state = {key: summary[key] for key in ("next_seq", "published", "accepted", "rejected", "accepted_bytes",
                                               "errors")}
        self._write_atomic(self.workdir / "state.json", json.dumps(state).encode())
        self._write_atomic(self.workdir / f"snapshot-{snapshot}.json", json.dumps(summary).encode())

    def _stats_loop(self) -> None:
        with open(self.workdir / "stats.jsonl", "a") as out:
            while not self.stopping:
                time.sleep(1.0)
                with self.lock:
                    line = {"time": time.time(), "pid": os.getpid(), "rate": self.rate, "next_seq": self.next_seq,
                            "published": self.published, "accepted": self.accepted, "rejected": self.rejected,
                            "accepted_bytes": self.accepted_bytes, "outstanding": self.outstanding,
                            "lag_s": round(self.lag_s, 3), "latency_hist": self.interval_latencies}
                    self.interval_latencies = {}
                out.write(json.dumps(line) + "\n")
                out.flush()

    @staticmethod
    def _read_json(path: Path):
        try:
            return json.loads(path.read_text())
        except (OSError, ValueError):
            return None

    @staticmethod
    def _write_atomic(path: Path, data: bytes) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workdir", required=True, type=Path)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-outstanding", type=int, default=32)
    parser.add_argument("--profile", default="{}", help="payload profile as JSON")
    args = parser.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    simulator = Simulator(args.workdir, args.topic, args.seed, args.max_outstanding,
                          PayloadProfile.from_dict(json.loads(args.profile)))

    def on_sigterm(*_):
        simulator.stopping = True

    signal.signal(signal.SIGTERM, on_sigterm)
    try:
        simulator.run()
    except Exception:
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
