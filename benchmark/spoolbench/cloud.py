"""
Cloud side: subscribes to the measurement topic on the broker (never through the network proxy), decodes each
protobuf batch and accounts for every sequence number: delivered, duplicated, out of order, end-to-end latency.
"""

import threading
import time
import uuid
from typing import Dict, List

import paho.mqtt.client as mqtt

from .payloads import PayloadDecoder
from .simulator import latency_bucket


class Cloud:
    def __init__(self, port: int, topic: str) -> None:
        self.port = port
        self.topic = topic
        self.decoder = PayloadDecoder()
        self.lock = threading.Lock()
        self.delivered_bits = bytearray(1 << 20)
        self.unique = 0
        self.duplicates = 0
        self.out_of_order = 0
        self.undecodable = 0
        self.bytes = 0
        self.max_seq = -1
        self.first_delivery_after: Dict[str, float] = {}
        self.latency_hist: Dict[int, int] = {}  # end-to-end, for unique deliveries, since the last drain
        self.last_unique_time = 0.0
        self._subscribed = threading.Event()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"spoolbench-cloud-{uuid.uuid4().hex[:6]}",
                                  protocol=mqtt.MQTTv311, clean_session=True)
        self.client.max_inflight_messages_set(1000)
        self.client.on_connect = self._on_connect
        self.client.on_subscribe = lambda *a, **k: self._subscribed.set()
        self.client.on_message = self._on_message

    def start(self) -> None:
        self.client.connect("127.0.0.1", self.port, keepalive=30)
        self.client.loop_start()
        if not self._subscribed.wait(30):
            raise TimeoutError("cloud subscriber could not subscribe")

    def stop(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        client.subscribe(self.topic, qos=1)

    def _on_message(self, client, userdata, message) -> None:
        now = time.time()
        try:
            seq, sent_ms = self.decoder.decode(message.payload)
        except Exception:  # noqa: BLE001 - counted
            with self.lock:
                self.undecodable += 1
            return
        with self.lock:
            self.bytes += len(message.payload)
            byte = seq >> 3
            if byte >= len(self.delivered_bits):
                self.delivered_bits.extend(bytearray(max(byte + 1 - len(self.delivered_bits), 1 << 20)))
            mask = 1 << (seq & 7)
            if self.delivered_bits[byte] & mask:
                self.duplicates += 1
                return
            self.delivered_bits[byte] |= mask
            self.unique += 1
            self.last_unique_time = now
            if seq < self.max_seq:
                self.out_of_order += 1
            self.max_seq = max(self.max_seq, seq)
            bucket = latency_bucket(max(now - sent_ms / 1000.0, 0.0))
            self.latency_hist[bucket] = self.latency_hist.get(bucket, 0) + 1

    # --- queries --------------------------------------------------------------------------------------------------

    def counters(self) -> Dict[str, int]:
        with self.lock:
            return {"unique": self.unique, "duplicates": self.duplicates, "out_of_order": self.out_of_order,
                    "undecodable": self.undecodable, "bytes": self.bytes}

    def take_latency_histogram(self) -> Dict[int, int]:
        with self.lock:
            hist, self.latency_hist = self.latency_hist, {}
            return hist

    def missing(self, accepted_bits: bytes, below_seq: int) -> List[int]:
        """Sequence numbers accepted by the Nucleus, below `below_seq`, that never reached the cloud."""
        with self.lock:
            delivered = bytes(self.delivered_bits[: len(accepted_bits)])
        missing = []
        for byte_index, accepted in enumerate(accepted_bits):
            lost = accepted & ~(delivered[byte_index] if byte_index < len(delivered) else 0)
            if lost:
                for bit in range(8):
                    seq = (byte_index << 3) + bit
                    if lost & (1 << bit) and seq < below_seq:
                        missing.append(seq)
        return missing
