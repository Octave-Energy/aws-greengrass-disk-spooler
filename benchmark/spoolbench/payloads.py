"""
Deterministic generator of IBEMS-like protobuf measurement batches.

IBEMS publishes `MeasurementMessage` batches built by its measurement dispatcher: most are small low-latency batches
(a handful of devices with a few recent values), some are larger, and BMS cell dumps (string -> module -> cell tree)
can approach the 125 KB per-message cap IBEMS applies for AWS IoT Core.

The generator builds a pool of distinct messages from a fixed seed, so every run, on every branch, publishes exactly
the same byte stream. Each published message gets:

- `batch_v1.timestamp_ms` = wall-clock time of sending (as IBEMS does), used for end-to-end latency, and
- a measurement of the `IBEMS` device named `bench_seq` holding the message sequence number, used by the cloud side to
  detect lost, duplicated and reordered messages. It is a valid measurement of the schema, a few bytes long.
"""

import random
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from . import protos

SEQ_SERIES_NAME = "bench_seq"
MAX_MESSAGE_BYTES = 125 * 1024  # same cap as IBEMS' MultiBatchBuilder

# measurement names per device family, close to what IBEMS emits
_SERIES = {
    "SITE_POWER_METER": ["power_kw", "power_l1_kw", "power_l2_kw", "power_l3_kw", "voltage_l1_v", "voltage_l2_v",
                         "voltage_l3_v", "current_l1_a", "current_l2_a", "current_l3_a", "frequency_hz",
                         "energy_import_kwh", "energy_export_kwh", "power_factor"],
    "INVERTER": ["power_kw", "reactive_power_kvar", "setpoint_kw", "dc_voltage_v", "dc_current_a", "temperature_c",
                 "state", "alarm_code", "max_charge_kw", "max_discharge_kw", "grid_frequency_hz"],
    "BMS": ["soc_pct", "soh_pct", "voltage_v", "current_a", "max_cell_voltage_v", "min_cell_voltage_v",
            "max_cell_temperature_c", "min_cell_temperature_c", "contactor_closed", "alarm_flags"],
    "PV_CLIENT": ["power_kw", "curtailment_pct", "setpoint_kw", "irradiance_w_m2", "energy_kwh"],
    "EMSCONTROLLER": ["target_power_kw", "mode", "soc_target_pct", "control_loop_ms", "limit_reason"],
    "EV_METER": ["power_kw", "session_energy_kwh", "connected", "current_a"],
    "AIRCO": ["temperature_c", "setpoint_c", "running", "power_kw"],
}
_DEVICE_TYPES_SMALL = ["SITE_POWER_METER", "INVERTER", "BMS", "PV_CLIENT", "EMSCONTROLLER", "EV_METER", "AIRCO"]
_STRING_VALUES = ["idle", "charging", "discharging", "standby", "fault", "grid_following", "peak_shaving"]


@dataclass(frozen=True)
class PayloadProfile:
    """Share of each batch kind in the generated stream."""

    small: float = 0.85  # low-latency batch: 1-4 devices, few recent values (~0.2-3 KB)
    medium: float = 0.13  # high-latency batch: more devices and values (~3-20 KB)
    large: float = 0.02  # BMS cell dump: string/module/cell tree (~20-125 KB)

    @staticmethod
    def from_dict(d: Optional[Dict]) -> "PayloadProfile":
        return PayloadProfile(**(d or {}))


class PayloadPool:
    """A fixed, seeded pool of messages that is cycled through; see the module docstring."""

    def __init__(self, seed: int, size: int = 2000, profile: PayloadProfile = PayloadProfile()) -> None:
        self.message_pb2, self.device_type_pb2, self.timed_value_pb2 = protos.load()
        rng = random.Random(seed)
        self.messages = [self._build(rng, profile) for _ in range(size)]
        # serialized size of each pooled message, with a 0 timestamp and sequence; real sizes differ by a few bytes
        self.sizes = [len(m.SerializeToString()) for m in self.messages]

    def payload(self, seq: int, timestamp_ms: int) -> bytes:
        """The serialized message for sequence number `seq`, stamped with `timestamp_ms`."""
        message = self.messages[seq % len(self.messages)]
        message.batch_v1.timestamp_ms = timestamp_ms
        seq_value = message.batch_v1.measurements[-1].series[0].values[0]
        seq_value.uint32_value = seq
        return message.SerializeToString()

    # --- generation -------------------------------------------------------------------------------------------

    def _build(self, rng: random.Random, profile: PayloadProfile):
        message = self.message_pb2.MeasurementMessage()
        batch = message.batch_v1
        batch.timestamp_ms = 0
        kind = rng.choices(["small", "medium", "large"], weights=[profile.small, profile.medium, profile.large])[0]
        if kind == "small":
            self._add_devices(rng, batch, devices=rng.randint(1, 4), series=(3, 8), values=(1, 5), span_ms=2_000)
        elif kind == "medium":
            self._add_devices(rng, batch, devices=rng.randint(4, 10), series=(5, 14), values=(5, 30), span_ms=60_000)
        else:
            self._add_bms_cells(rng, batch)
        self._add_sequence_series(batch)
        while len(message.SerializeToString()) > MAX_MESSAGE_BYTES:  # IBEMS splits; we just trim the biggest device
            biggest = max(range(len(batch.measurements) - 1), key=lambda i: batch.measurements[i].ByteSize())
            series = batch.measurements[biggest].series
            del series[len(series) - 1]
        return message

    def _add_devices(self, rng, batch, devices: int, series: Tuple[int, int], values: Tuple[int, int], span_ms: int):
        for device_name in rng.sample(_DEVICE_TYPES_SMALL, k=min(devices, len(_DEVICE_TYPES_SMALL))):
            device = batch.measurements.add()
            device.device_type = self.device_type_pb2.DeviceType.Value(device_name)
            if device_name in ("INVERTER", "BMS", "PV_CLIENT") and rng.random() < 0.5:
                device.device_index = rng.randint(0, 3)
            names = _SERIES[device_name]
            for name in rng.sample(names, k=min(rng.randint(*series), len(names))):
                named = device.series.add()
                named.name = name
                self._add_values(rng, named.values, rng.randint(*values), span_ms, name)

    def _add_bms_cells(self, rng, batch):
        device = batch.measurements.add()
        device.device_type = self.device_type_pb2.DeviceType.Value("BMS")
        device.device_index = rng.randint(0, 1)
        device.bms_index = rng.randint(0, 3)
        strings, modules, cells, samples = rng.randint(1, 4), rng.randint(8, 20), rng.randint(12, 16), rng.randint(1, 3)
        for name in ["cell_voltage_v", "cell_temperature_c"]:
            named = device.series.add()
            named.name = name
            for string_id in range(strings):
                string = named.string_measurements.add()
                string.string_id = string_id
                for module_id in range(modules):
                    module = string.module_measurements.add()
                    module.module_id = module_id
                    for cell_id in range(cells):
                        cell = module.cell_measurements.add()
                        cell.cell_id = cell_id
                        self._add_values(rng, cell.values, samples, 60_000, name)

    def _add_values(self, rng, values, count: int, span_ms: int, name: str):
        offsets = sorted(rng.randrange(0, span_ms) for _ in range(count))
        for offset in offsets:
            value = values.add()
            value.time_offset_ms = offset
            roll = rng.random()
            if name in ("state", "mode", "limit_reason"):
                value.string_value = rng.choice(_STRING_VALUES)
            elif name in ("contactor_closed", "connected", "running"):
                value.bool_value = rng.random() < 0.5
            elif name in ("alarm_code", "control_loop_ms"):
                value.uint32_value = rng.randint(0, 5000)
            elif name == "alarm_flags":
                value.multi_int32_value.values.extend(rng.randint(0, 255) for _ in range(rng.randint(1, 6)))
            elif roll < 0.15:
                value.double_value = rng.uniform(-1e5, 1e5)
            else:
                value.float_value = rng.uniform(-500.0, 500.0)

    def _add_sequence_series(self, batch):
        device = batch.measurements.add()
        device.device_type = self.device_type_pb2.DeviceType.Value("IBEMS")
        named = device.series.add()
        named.name = SEQ_SERIES_NAME
        value = named.values.add()
        value.time_offset_ms = 0
        value.uint32_value = 0


class PayloadDecoder:
    """Cloud-side decoding: extracts (sequence number, send timestamp) from a published payload."""

    def __init__(self) -> None:
        self.message_pb2, self.device_type_pb2, _ = protos.load()
        self.ibems_device = self.device_type_pb2.DeviceType.Value("IBEMS")

    def decode(self, payload: bytes) -> Tuple[int, int]:
        message = self.message_pb2.MeasurementMessage.FromString(payload)
        batch = message.batch_v1
        for device in reversed(batch.measurements):
            if device.device_type == self.ibems_device:
                for series in device.series:
                    if series.name == SEQ_SERIES_NAME:
                        return series.values[0].uint32_value, batch.timestamp_ms
        raise ValueError("payload has no bench_seq measurement")


def size_stats(sizes: List[int]) -> Dict[str, float]:
    ordered = sorted(sizes)

    def pct(p: float) -> int:
        return ordered[min(len(ordered) - 1, int(p * len(ordered)))]

    return {
        "count": len(ordered),
        "mean_bytes": round(sum(ordered) / len(ordered), 1),
        "p50_bytes": pct(0.50),
        "p95_bytes": pct(0.95),
        "p99_bytes": pct(0.99),
        "max_bytes": ordered[-1],
    }
