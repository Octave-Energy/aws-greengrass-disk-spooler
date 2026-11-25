"""
The "cloud" MQTT broker: Eclipse Mosquitto, run from a pinned Docker image (or a local `mosquitto` binary).

Two listeners:
- a mutual-TLS listener the Nucleus connects to (through the network proxy), like AWS IoT Core, and
- a plain listener on localhost for the cloud-side subscriber, which never goes through the proxy.

Both allow anonymous clients, so they must only be reachable from this machine: a local `mosquitto` binds them to
127.0.0.1; in Docker they listen inside the container and are published on 127.0.0.1 only.
"""

import random
import shutil
import socket
import subprocess
import time
import uuid
from pathlib import Path
from typing import List, Optional

from .certs import Certs

DEFAULT_IMAGE = "eclipse-mosquitto:2.0.20"


def free_port(low: int = 20000, high: int = 32000) -> int:
    """A free TCP port below 32768: the Nucleus casts the MQTT port to a Java short (`withPort((short) port)`)."""
    for port in random.sample(range(low, high), 200):
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"no free port in [{low}, {high})")


def wait_for_port(port: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise TimeoutError(f"nothing listening on port {port} after {timeout_s}s")


class Broker:
    def __init__(self, workdir: Path, certs: Certs, image: str = DEFAULT_IMAGE, binary: Optional[str] = None) -> None:
        self.workdir = workdir
        self.certs = certs
        self.image = image
        self.binary = binary
        self.tls_port = free_port()
        self.plain_port = free_port()
        self.container: Optional[str] = None
        self.process: Optional[subprocess.Popen] = None

    @staticmethod
    def _config(cert_dir: str, tls_port: int, plain_port: int, bind_address: Optional[str]) -> str:
        bind = f" {bind_address}" if bind_address else ""
        return "\n".join([
            "persistence false",
            "log_dest stdout",
            "log_type error",
            "log_type warning",
            # never drop QoS 1 messages queued for a slow subscriber, nor limit their number in flight
            "max_queued_messages 0",
            "max_queued_bytes 0",
            "max_inflight_messages 0",
            "per_listener_settings true",
            f"listener {tls_port}{bind}",
            f"cafile {cert_dir}/ca.pem",
            f"certfile {cert_dir}/server.pem",
            f"keyfile {cert_dir}/server.key",
            "require_certificate true",
            "use_identity_as_username true",
            "allow_anonymous true",
            f"listener {plain_port}{bind}",
            "allow_anonymous true",
            "",
        ])

    def start(self) -> None:
        self.workdir.mkdir(parents=True, exist_ok=True)
        log = open(self.workdir / "mosquitto.log", "ab")
        if self.binary or (shutil.which("docker") is None and shutil.which("mosquitto")):
            binary = self.binary or shutil.which("mosquitto")
            conf = self.workdir / "mosquitto.conf"
            # no port mapping in front of a local broker: listen on localhost only
            conf.write_text(self._config(str(self.certs.ca_cert.parent), self.tls_port, self.plain_port, "127.0.0.1"))
            self.process = subprocess.Popen([binary, "-c", str(conf)], stdout=log, stderr=subprocess.STDOUT)
        else:
            conf = self.workdir / "mosquitto.conf"
            # all interfaces of the container, which Docker publishes on the host's 127.0.0.1 only (-p below)
            conf.write_text(self._config("/certs", 8883, 1883, None))
            self.container = f"spoolbench-broker-{uuid.uuid4().hex[:8]}"
            cmd = ["docker", "run", "-d", "--rm", "--name", self.container,
                   "-p", f"127.0.0.1:{self.tls_port}:8883", "-p", f"127.0.0.1:{self.plain_port}:1883",
                   "-v", f"{self.certs.ca_cert.parent}:/certs:ro",
                   "-v", f"{conf}:/mosquitto/config/mosquitto.conf:ro",
                   self.image]
            subprocess.run(cmd, check=True, capture_output=True)
        wait_for_port(self.plain_port, 60)
        wait_for_port(self.tls_port, 60)

    def versions(self) -> List[str]:
        return [self.binary or (self.image if self.container else "mosquitto")]

    def stop(self) -> None:
        if self.container:
            with open(self.workdir / "mosquitto.log", "ab") as log:
                subprocess.run(["docker", "logs", self.container], stdout=log, stderr=subprocess.STDOUT)
            subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)
            self.container = None
        if self.process:
            self.process.terminate()
            self.process.wait(10)
            self.process = None
