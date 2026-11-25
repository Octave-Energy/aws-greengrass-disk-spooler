"""Generates and imports the IBEMS measurement protobuf stubs."""

import importlib
import sys
from pathlib import Path

BENCHMARK_DIR = Path(__file__).resolve().parent.parent
PROTO_DIR = BENCHMARK_DIR / "proto"
GENERATED_DIR = BENCHMARK_DIR / ".generated"


def generate(force: bool = False) -> Path:
    """Compiles the .proto files into Python stubs under benchmark/.generated (skipped when up to date)."""
    protos = sorted(PROTO_DIR.rglob("*.proto"))
    marker = GENERATED_DIR / "ibems_measurement" / "message_pb2.py"
    newest_proto = max(p.stat().st_mtime for p in protos)
    if not force and marker.exists() and marker.stat().st_mtime >= newest_proto:
        return GENERATED_DIR

    from grpc_tools import protoc  # imported lazily: only needed to generate

    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    args = ["protoc", f"-I{PROTO_DIR}", f"--python_out={GENERATED_DIR}"] + [str(p) for p in protos]
    if protoc.main(args) != 0:
        raise RuntimeError(f"protoc failed for {PROTO_DIR}")
    for package_dir in [GENERATED_DIR / "ibems_measurement", GENERATED_DIR / "ibems_measurement" / "v1"]:
        (package_dir / "__init__.py").touch()
    return GENERATED_DIR


def load():
    """Returns the generated modules: (message_pb2, measurement_batch_pb2, ...) as a simple namespace."""
    generated = generate()
    if str(generated) not in sys.path:
        sys.path.insert(0, str(generated))
    message_pb2 = importlib.import_module("ibems_measurement.message_pb2")
    device_type_pb2 = importlib.import_module("ibems_measurement.v1.device_type_pb2")
    timed_value_pb2 = importlib.import_module("ibems_measurement.v1.timed_value_pb2")
    return message_pb2, device_type_pb2, timed_value_pb2
