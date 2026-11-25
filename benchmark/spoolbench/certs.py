"""Throw-away test PKI (CA, broker certificate for `localhost`, device certificate) created with the openssl CLI."""

import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Certs:
    ca_cert: Path
    server_cert: Path
    server_key: Path
    device_cert: Path
    device_key: Path


def _openssl(*args: str, cwd: Path) -> None:
    subprocess.run(["openssl", *args], cwd=cwd, check=True, capture_output=True)


def create(directory: Path) -> Certs:
    directory.mkdir(parents=True, exist_ok=True)
    certs = Certs(
        ca_cert=directory / "ca.pem",
        server_cert=directory / "server.pem",
        server_key=directory / "server.key",
        device_cert=directory / "device.pem",
        device_key=directory / "device.key",
    )
    if all(p.exists() for p in vars(certs).values()):
        return certs

    _openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "30", "-subj", "/CN=spoolbench-test-ca",
             "-keyout", "ca.key", "-out", "ca.pem", cwd=directory)
    # macOS' TLS stack (used by the AWS CRT on a Mac) requires the extended key usage on server certificates
    (directory / "server.ext").write_text("subjectAltName=DNS:localhost,IP:127.0.0.1\n"
                                          "extendedKeyUsage=serverAuth\nkeyUsage=digitalSignature,keyEncipherment\n")
    (directory / "device.ext").write_text("extendedKeyUsage=clientAuth\nkeyUsage=digitalSignature,keyEncipherment\n")
    for name, subject, extra in [("server", "/CN=localhost", ["-extfile", "server.ext"]),
                                 ("device", "/CN=spoolbench-device", ["-extfile", "device.ext"])]:
        _openssl("req", "-newkey", "rsa:2048", "-nodes", "-subj", subject, "-keyout", f"{name}.key",
                 "-out", f"{name}.csr", cwd=directory)
        _openssl("x509", "-req", "-in", f"{name}.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial",
                 "-days", "30", "-out", f"{name}.pem", *extra, cwd=directory)
    # test-only keys: readable by the broker container's user
    for p in directory.iterdir():
        p.chmod(0o644)
    return certs
