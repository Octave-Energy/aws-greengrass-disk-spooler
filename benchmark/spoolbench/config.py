"""Benchmark profiles (scenarios/*.yaml): production settings plus the list of scenarios."""

import itertools
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml


@dataclass
class Scenario:
    name: str
    rate: float  # messages per second published by the simulator
    spill_threshold_bytes: int  # diskSpillThresholdBytes of the plugin (0: everything goes to disk)
    online_s: float = 30
    outage_s: float = 60
    outage_mode: str = "blackhole"  # or "reset"
    device_crash: bool = False
    offline_after_reboot_s: float = 15
    warmup_s: float = 5
    max_drain_s: float = 300
    stall_s: float = 60
    cooldown_s: float = 30  # cooldown ends when nothing more is delivered for this long (the rest is lost)
    max_cooldown_s: float = 1200  # ... or after this long (the rest is reported as undelivered, not lost)


@dataclass
class Settings:
    nucleus_version: str = "2.18.3"
    nucleus_configuration: Dict[str, Any] = field(default_factory=dict)
    plugin_configuration: Dict[str, Any] = field(default_factory=dict)
    jvm_options: List[str] = field(default_factory=list)
    seed: int = 42
    max_outstanding: int = 32
    payload_profile: Dict[str, float] = field(default_factory=dict)
    java_home: Optional[str] = None
    broker_image: Optional[str] = None
    startup_timeout_s: float = 180
    connect_timeout_s: float = 180


def scenario_name(params: Dict[str, Any]) -> str:
    spill = params["spill_threshold_bytes"]
    spill_text = f"{spill // 1024}k" if spill and spill % 1024 == 0 else str(spill)
    name = f"r{params['rate']:g}-spill{spill_text}"
    if params.get("device_crash"):
        name += "-devicecrash"
    if params.get("outage_mode", "blackhole") != "blackhole":
        name += f"-{params['outage_mode']}"
    return name


def load(path: Path) -> Tuple[Dict[str, Any], Settings, List[Scenario]]:
    profile = yaml.safe_load(path.read_text())
    settings_keys = {f.name for f in fields(Settings)}
    settings = Settings(**{k: v for k, v in profile.items() if k in settings_keys})
    defaults = profile.get("defaults", {})
    combos: List[Dict[str, Any]] = []
    matrix = profile.get("matrix", {})
    if matrix:
        keys = list(matrix)
        for values in itertools.product(*(matrix[k] for k in keys)):
            combos.append(dict(zip(keys, values)))
    combos.extend(profile.get("scenarios", []))
    scenarios = []
    for combo in combos:
        params = {**defaults, **combo}
        params.setdefault("name", scenario_name(params))
        scenarios.append(Scenario(**params))
    return profile, settings, scenarios
