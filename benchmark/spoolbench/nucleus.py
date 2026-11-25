"""
A real AWS IoT Greengrass Nucleus, installed from the released distribution, with the disk spooler plugin.

The plugin is installed the way a cloud deployment installs it: jar in the component store's artifacts, recipe in its
recipes, and the recipe digest recorded under `services.main.runtime.service-digest`, so the Nucleus loads it in its
own class loader exactly as on a device (no class path shortcuts).
"""

import base64
import getpass
import grp
import hashlib
import json
import os
import shutil
import signal
import subprocess
import time
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import psutil
import yaml

from .certs import Certs

PLUGIN_NAME = "aws.greengrass.DiskSpooler"
PLUGIN_VERSION = "1.0.0-bench"  # same for every branch, so every run has an identical component store layout
SIMULATOR_NAME = "spoolbench.IbemsSimulator"
THING_NAME = "spoolbench-device"
DOWNLOAD_URL = "https://d2s8p88vqu9w66.cloudfront.net/releases/greengrass-{version}.zip"

PERSISTENT_SPOOLER_READY = "Persistent Spooler has been set up"
FELL_BACK_TO_MEMORY = "defaulting to InMemory Spooler"
# logged as an ERROR on every reconnect attempt while the link is down
MQTT_CONNECT_FAILED = "Failed to connect to AWS IoT Core"


def install_distribution(version: str, cache_dir: Path) -> Path:
    """Downloads and unzips the released Nucleus distribution (cached), returning the directory with lib/."""
    target = cache_dir / f"greengrass-{version}"
    if (target / "lib" / "Greengrass.jar").exists():
        return target
    cache_dir.mkdir(parents=True, exist_ok=True)
    archive = cache_dir / f"greengrass-{version}.zip"
    if not archive.exists():
        tmp = archive.with_suffix(".part")
        urllib.request.urlretrieve(DOWNLOAD_URL.format(version=version), tmp)
        tmp.rename(archive)
    with zipfile.ZipFile(archive) as z:
        z.extractall(target)
    if not (target / "lib" / "Greengrass.jar").exists():
        raise RuntimeError(f"{archive} does not contain lib/Greengrass.jar")
    return target


def _recipe_file_prefix(name: str, version: str) -> str:
    digest = hashlib.sha256(name.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=") + "@" + version


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


class Nucleus:
    def __init__(self, root: Path, distribution: Path, java_home: Optional[str], jvm_options: List[str],
                 log_dir: Path) -> None:
        self.root = root
        self.distribution = distribution
        self.java = str(Path(java_home) / "bin" / "java") if java_home else "java"
        self.jvm_options = jvm_options
        self.log_dir = log_dir
        self.process: Optional[subprocess.Popen] = None
        self.starts = 0

    # --- installation -------------------------------------------------------------------------------------------

    def install(self, plugin_jar: Path, certs: Certs, mqtt_port: int, nucleus_configuration: Dict[str, Any],
                plugin_configuration: Dict[str, Any], simulator_command: str, simulator_env: Dict[str, str]) -> Path:
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True)

        recipe = yaml.safe_dump({
            "RecipeFormatVersion": "2020-01-25",
            "ComponentName": PLUGIN_NAME,
            "ComponentVersion": PLUGIN_VERSION,
            "ComponentType": "aws.greengrass.plugin",
            "ComponentPublisher": "spoolbench",
            "Manifests": [{"Platform": {"os": "*"}, "Lifecycle": {},
                           "Artifacts": [{"URI": f"s3://spoolbench/{PLUGIN_NAME}.jar"}]}],
        }, sort_keys=False)
        recipes = self.root / "packages" / "recipes"
        artifacts = self.root / "packages" / "artifacts" / PLUGIN_NAME / PLUGIN_VERSION
        recipes.mkdir(parents=True)
        artifacts.mkdir(parents=True)
        (recipes / f"{_recipe_file_prefix(PLUGIN_NAME, PLUGIN_VERSION)}.recipe.yaml").write_text(recipe)
        shutil.copyfile(plugin_jar, artifacts / f"{PLUGIN_NAME}.jar")
        recipe_digest = base64.b64encode(hashlib.sha256(recipe.encode("utf-8")).digest()).decode("ascii")

        user = getpass.getuser()
        group = grp.getgrgid(os.getgid()).gr_name
        nucleus_config = _deep_merge({
            "awsRegion": "us-east-1",
            "iotDataEndpoint": "localhost",
            "iotCredEndpoint": "localhost",
            "runWithDefault": {"posixUser": f"{user}:{group}"},
            "mqtt": {"port": mqtt_port},
        }, nucleus_configuration)

        config = {
            "system": {
                "certificateFilePath": str(certs.device_cert),
                "privateKeyPath": str(certs.device_key),
                "rootCaPath": str(certs.ca_cert),
                "rootpath": str(self.root),
                "thingName": THING_NAME,
            },
            "services": {
                "aws.greengrass.Nucleus": {
                    "componentType": "NUCLEUS",
                    "configuration": nucleus_config,
                },
                PLUGIN_NAME: {
                    "componentType": "PLUGIN",
                    "version": PLUGIN_VERSION,
                    "configuration": plugin_configuration,
                },
                SIMULATOR_NAME: {
                    "componentType": "GENERIC",
                    "version": "1.0.0",
                    "lifecycle": {"setenv": simulator_env, "run": simulator_command},
                    "configuration": {
                        "accessControl": {
                            "aws.greengrass.ipc.mqttproxy": {
                                f"{SIMULATOR_NAME}:mqttproxy:1": {
                                    "policyDescription": "publish measurements to IoT Core",
                                    "operations": ["aws.greengrass#PublishToIoTCore"],
                                    "resources": ["*"],
                                }
                            }
                        }
                    },
                },
                "main": {
                    "dependencies": [PLUGIN_NAME, SIMULATOR_NAME],
                    "lifecycle": {},
                    "runtime": {"service-digest": {f"{PLUGIN_NAME}-v{PLUGIN_VERSION}": recipe_digest}},
                },
            },
        }
        init_config = self.root.parent / f"{self.root.name}-init-config.yaml"
        init_config.write_text(yaml.safe_dump(config, sort_keys=False))
        self.init_config = init_config
        return init_config

    # --- process control ----------------------------------------------------------------------------------------

    @property
    def greengrass_log(self) -> Path:
        return self.root / "logs" / "greengrass.log"

    @property
    def spooler_db(self) -> Path:
        return self.root / "work" / PLUGIN_NAME / "spooler.db"

    def start(self) -> int:
        """Starts the Nucleus (first start installs it from the init config) and returns the JVM pid."""
        self.starts += 1
        jar = self.distribution / "lib" / "Greengrass.jar"
        args = [self.java, *self.jvm_options, f"-Droot={self.root}", "-Dlog.store=FILE", "-jar", str(jar),
                "--root", str(self.root), "--setup-system-service", "false", "--start", "true"]
        if self.starts == 1:
            args += ["--init-config", str(self.init_config)]
        self.log_dir.mkdir(parents=True, exist_ok=True)
        stdout = open(self.log_dir / f"nucleus-stdout-{self.starts}.log", "wb")
        # own session, so that a "device crash" can kill the Nucleus and every component it started at once
        self.process = subprocess.Popen(args, stdout=stdout, stderr=subprocess.STDOUT, start_new_session=True,
                                        cwd=self.root)
        return self.process.pid

    def wait_until_spooler_ready(self, timeout_s: float, log_offset: int = 0) -> None:
        """Waits until the Nucleus has set up the persistent spooler, and fails if it fell back to memory."""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"Nucleus exited with {self.process.returncode}, see {self.log_dir}")
            text = self._log_since(log_offset)
            if FELL_BACK_TO_MEMORY in text:
                raise RuntimeError("Nucleus fell back to the in-memory spooler: the disk spooler plugin did not load")
            if PERSISTENT_SPOOLER_READY in text:
                return
            time.sleep(0.1)  # the startup time is a metric: poll often enough to measure it
        raise TimeoutError(f"persistent spooler not set up after {timeout_s}s, see {self.greengrass_log}")

    def log_size(self) -> int:
        return self.greengrass_log.stat().st_size if self.greengrass_log.exists() else 0

    def _log_since(self, offset: int) -> str:
        if not self.greengrass_log.exists():
            return ""
        with open(self.greengrass_log, "rb") as f:
            f.seek(offset)
            return f.read().decode("utf-8", errors="replace")

    def stop(self, timeout_s: float = 60) -> None:
        """Graceful shutdown (SIGTERM), as by systemd: the Nucleus stops its components and plugins."""
        if self.process is None or self.process.poll() is not None:
            return
        self.process.send_signal(signal.SIGTERM)
        try:
            self.process.wait(timeout_s)
        except subprocess.TimeoutExpired:
            self.kill()
        self._kill_group()

    def kill(self) -> None:
        """Device crash: the Nucleus and all its components die at once, without any shutdown (SIGKILL)."""
        if self.process is None:
            return
        try:  # components may run in their own process group: kill every descendant too
            descendants = psutil.Process(self.process.pid).children(recursive=True)
        except psutil.NoSuchProcess:
            descendants = []
        self._kill_group()
        for child in descendants:
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass
        self.process.wait(30)

    def _kill_group(self) -> None:
        try:
            os.killpg(self.process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def collect_logs(self) -> None:
        logs = self.root / "logs"
        if logs.exists():
            shutil.copytree(logs, self.log_dir / "greengrass-logs", dirs_exist_ok=True)

    def log_summary(self, top: int = 15) -> Dict[str, Any]:
        """Counts WARN/ERROR log events, grouped by logger and message (JSON or text log format), and separately the
        ERROR events of failed MQTT connect attempts."""
        counts: Dict[str, int] = {}
        totals = {"WARN": 0, "ERROR": 0}
        mqtt_connect_errors = 0
        logs = sorted((self.root / "logs").glob("*.log")) if (self.root / "logs").exists() else []
        for log in logs:
            for line in log.read_text(errors="replace").splitlines():
                level, key = None, None
                if line.startswith("{"):
                    try:
                        event = json.loads(line)
                    except ValueError:
                        continue
                    level = event.get("level")
                    key = f"{log.stem}: {event.get('loggerName', '')}: {event.get('eventType') or ''} " \
                          f"{(event.get('message') or '')[:120]}"
                else:
                    for lvl in ("WARN", "ERROR"):
                        if f"[{lvl}]" in line:
                            level, key = lvl, f"{log.stem}: {line.split(']', 2)[-1][:160]}"
                if level in totals:
                    totals[level] += 1
                    if level == "ERROR" and MQTT_CONNECT_FAILED in line:
                        mqtt_connect_errors += 1
                    counts[f"[{level}] {key.strip()}"] = counts.get(f"[{level}] {key.strip()}", 0) + 1
        ordered = sorted(counts.items(), key=lambda kv: -kv[1])[:top]
        return {"warn": totals["WARN"], "error": totals["ERROR"], "error_mqtt_connect": mqtt_connect_errors,
                "top": [{"count": c, "event": e} for e, c in ordered]}
