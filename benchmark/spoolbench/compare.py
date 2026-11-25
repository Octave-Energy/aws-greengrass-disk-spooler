"""
Compares two builds (labels) scenario by scenario, from one report.json or from two.

Every headline metric has a direction (lower or higher is better, or informational) and a noise tolerance: a change
within max(relative tolerance x baseline, absolute tolerance) is reported as unchanged.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

KIB = 1024
MIB = 1024 * 1024


@dataclass(frozen=True)
class Metric:
    group: str
    key: str
    title: str
    unit: str  # bytes, ms, s, rate, pct, count, ratio, bool
    better: Optional[str]  # "lower", "higher" or None (informational)
    rel_tol: float = 0.1
    abs_tol: float = 0.0


METRICS: List[Metric] = [
    Metric("Ingest", "ingest_rate_msg_s", "accepted by the Nucleus, msg/s (online)", "rate", "higher", 0.05, 1),
    Metric("Ingest", "ingest_shortfall_pct", "shortfall vs. target rate (online)", "pct", "lower", 0.2, 2),
    Metric("Ingest", "ipc_latency_p50_ms", "IPC publish latency p50 (online)", "ms", "lower", 0.25, 0.5),
    Metric("Ingest", "ipc_latency_p99_ms", "IPC publish latency p99 (online)", "ms", "lower", 0.5, 10),
    Metric("Ingest", "cpu_pct_online", "Nucleus CPU % of one core (online)", "pct", "lower", 0.15, 3),
    Metric("Delivery", "delivery_rate_online_msg_s", "delivered to the cloud, msg/s (online)", "rate", "higher", 0.05,
           1),
    Metric("Delivery", "backlog_online_end_msgs", "backlog at end of online phase", "count", "lower", 0.25, 20),
    Metric("Delivery", "e2e_latency_p50_ms", "end-to-end latency p50 (online)", "ms", "lower", 0.25, 5),
    Metric("Delivery", "e2e_latency_p99_ms", "end-to-end latency p99 (online)", "ms", "lower", 0.5, 20),
    Metric("Delivery", "recovery_delivery_rate_msg_s", "delivered msg/s while catching up", "rate", "higher", 0.1, 2),
    Metric("Delivery", "cooldown_delivery_rate_msg_s", "delivery capacity, msg/s (backlog, no input)", "rate",
           "higher", 0.1, 2),
    Metric("Outage", "outage_accepted_msgs", "messages accepted during the outage", "count", None),
    Metric("Outage", "outage_accepted_payload_bytes", "payload accepted during the outage", "bytes", None),
    Metric("Outage", "outage_rejected_msgs", "publishes rejected during the outage", "count", "lower", 0, 0),
    Metric("Outage", "ipc_latency_p99_ms_outage", "IPC publish latency p99 (outage)", "ms", "lower", 0.5, 10),
    Metric("Outage", "cpu_pct_outage", "Nucleus CPU % of one core (outage)", "pct", "lower", 0.25, 3),
    Metric("Disk", "disk_db_growth_outage_bytes", "spooler.db growth during the outage", "bytes", "lower", 0.1,
           64 * KIB),
    Metric("Disk", "disk_bytes_per_payload_byte", "disk bytes per spooled payload byte", "ratio", "lower", 0.05, 0.02),
    Metric("Disk", "disk_db_bytes_peak", "spooler.db peak size", "bytes", "lower", 0.1, 64 * KIB),
    Metric("Disk", "disk_db_bytes_after_stop", "spooler.db size after drain and stop", "bytes", "lower", 0.1, 64 * KIB),
    Metric("Disk", "disk_write_bytes_outage", "bytes written to storage during the outage (Linux)", "bytes", "lower",
           0.15, MIB),
    Metric("Disk", "disk_write_amplification", "storage bytes written per payload byte (Linux)", "ratio", "lower",
           0.1, 0.1),
    Metric("Memory", "heap_backlog_bytes", "live heap held by the backlog (outage end - online)", "bytes", "lower",
           0.2, 512 * KIB),
    Metric("Memory", "spool_messages_in_heap_outage_end", "spool messages in the heap (outage end)", "count", "lower",
           0.2, 10),
    Metric("Memory", "heap_live_online_bytes", "live heap, online", "bytes", "lower", 0.15, MIB),
    Metric("Memory", "heap_live_drained_bytes", "live heap after the drain", "bytes", "lower", 0.15, MIB),
    Metric("Memory", "heap_used_peak_bytes", "heap used peak (incl. garbage, depends on GC timing)", "bytes", None),
    Metric("Memory", "rss_peak_bytes", "Nucleus RSS peak", "bytes", "lower", 0.15, 16 * MIB),
    Metric("Memory", "rss_growth_outage_bytes", "RSS growth during the outage (depends on GC timing)", "bytes", None),
    Metric("Recovery", "recovery_backlog_msgs", "backlog when the link came back", "count", None),
    Metric("Recovery", "recovery_first_delivery_s", "first delivery after the link came back", "s", "lower", 0.5, 1),
    Metric("Recovery", "recovery_drain_s", "time to deliver the outage backlog", "s", "lower", 0.15, 2),
    Metric("Recovery", "recovery_drain_rate_msg_s", "outage backlog / drain time, msg/s", "rate", "higher", 0.1, 2),
    Metric("Recovery", "recovery_drained", "outage backlog fully delivered", "bool", "higher", 0, 0),
    Metric("Recovery", "backlog_recovery_end_msgs", "backlog at end of recovery", "count", "lower", 0.25, 20),
    Metric("Integrity", "lost_msgs", "accepted but never delivered (lost)", "count", "lower", 0, 0),
    Metric("Integrity", "undelivered_msgs", "still undelivered when the benchmark stopped waiting", "count", "lower",
           0, 0),
    Metric("Integrity", "duplicate_msgs", "duplicates delivered", "count", "lower", 0.5, 10),
    Metric("Integrity", "rejected_msgs", "publishes rejected (total)", "count", "lower", 0, 0),
    Metric("Integrity", "out_of_order_msgs", "delivered out of order", "count", None),
    Metric("Robustness", "startup_s", "Nucleus start until spooler ready", "s", "lower", 0.3, 1),
    Metric("Robustness", "reboot_s", "start after the device crash until spooler ready", "s", "lower", 0.3, 1),
    Metric("Robustness", "log_errors", "ERROR log events", "count", "lower", 0, 0),
    Metric("Robustness", "log_warnings", "WARN log events", "count", "lower", 0.5, 5),
    Metric("Robustness", "log_disk_add_failed_fallback_to_memory", "'Disk Spooler failed to add Message' events",
           "count", "lower", 0, 0),
    Metric("Robustness", "log_spool_full_rejected", "'Message spool is full' events", "count", None),
    Metric("Robustness", "log_db_corrupted_recreated", "database corrupted and recreated", "count", "lower", 0, 0),
]
METRICS_BY_KEY = {m.key: m for m in METRICS}


def fmt(value: Any, unit: str) -> str:
    if value is None:
        return "–"
    if unit == "bool":
        return "yes" if value else "no"
    if unit == "bytes":
        if abs(value) >= 1024 * MIB:
            return f"{value / (1024 * MIB):,.1f} GiB"
        if abs(value) >= MIB:
            return f"{value / MIB:,.1f} MiB"
        if abs(value) >= KIB:
            return f"{value / KIB:,.0f} KiB"
        return f"{value:,.0f} B"
    if unit == "ms":
        return f"{value:,.1f} ms" if abs(value) < 100 else f"{value:,.0f} ms"
    if unit == "s":
        return f"{value:,.1f} s"
    if unit == "rate":
        return f"{value:,.1f}"
    if unit == "pct":
        return f"{value:,.1f}%"
    if unit == "ratio":
        return f"{value:,.2f}"
    return f"{value:,.0f}" if isinstance(value, (int, float)) and abs(value - round(value)) < 1e-9 else f"{value:,.1f}"


def verdict(metric: Metric, base: Any, cand: Any) -> Tuple[str, Optional[float]]:
    """('better' | 'worse' | 'same' | 'info' | 'n/a', relative change)"""
    if base is None or cand is None:
        return ("n/a" if base is not None or cand is not None else "same"), None
    if metric.unit == "bool":
        if bool(base) == bool(cand) or metric.better is None:
            return "same", None
        return ("better" if bool(cand) else "worse"), None
    diff = cand - base
    relative = diff / abs(base) if base else None
    if metric.better is None:
        return "info", relative
    if abs(diff) <= max(metric.abs_tol, metric.rel_tol * abs(base)):
        return "same", relative
    improved = diff < 0 if metric.better == "lower" else diff > 0
    return ("better" if improved else "worse"), relative


SYMBOL = {"better": "🟢 better", "worse": "🔴 worse", "same": "≈", "info": "", "n/a": "n/a"}


def aggregate(results: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """scenario -> mean summary over the repeats (and the number of runs / failures)."""
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for result in results:
        grouped.setdefault(result["scenario"], []).append(result)
    aggregated = {}
    for scenario, runs in grouped.items():
        ok = [r for r in runs if r.get("status") == "ok"]
        summary: Dict[str, Any] = {}
        for key in (ok[0]["summary"] if ok else {}):
            values = [r["summary"].get(key) for r in ok if r["summary"].get(key) is not None]
            if not values:
                summary[key] = None
            elif isinstance(values[0], bool):
                summary[key] = all(values)
            else:
                summary[key] = sum(values) / len(values)
        params = dict(runs[0].get("scenario_params") or {})
        params.pop("name", None)
        aggregated[scenario] = {"summary": summary, "runs": len(runs), "failed": len(runs) - len(ok),
                                "errors": [r.get("error") for r in runs if r.get("status") != "ok"],
                                "params": params}
    return aggregated


def scenario_sort_key(name: str, params: Optional[Dict[str, Any]]) -> Tuple:
    """Rate first (10, 50, 100 msg/s), then plain outage before crash and reset variants, then spill threshold."""
    params = params or {}
    crash = bool(params.get("device_crash") or params.get("power_loss"))
    reset = params.get("outage_mode", "blackhole") != "blackhole"
    return (float(params.get("rate", 0)), crash, reset, params.get("spill_threshold_bytes", 0), name)


def _results_of(report: Dict[str, Any], label: str) -> List[Dict[str, Any]]:
    return [r for r in report["results"] if r["label"] == label]


# profile settings that change the workload or the system under test: reports that differ in any of them do not
# measure the same thing
PROFILE_CONDITIONS = ("nucleus_version", "nucleus_configuration", "plugin_configuration", "jvm_options", "seed",
                      "max_outstanding", "payload_profile")


def _differences(base: Dict[str, Any], cand: Dict[str, Any], keys) -> List[str]:
    return [f"`{key}` {json.dumps(base.get(key))} ≠ {json.dumps(cand.get(key))}" for key in keys
            if base.get(key) != cand.get(key)]


def render(baseline_report: Dict[str, Any], baseline_label: str, candidate_report: Dict[str, Any],
           candidate_label: str) -> Tuple[str, int]:
    base = aggregate(_results_of(baseline_report, baseline_label))
    cand = aggregate(_results_of(candidate_report, candidate_label))
    scenarios = sorted(set(base) | set(cand), key=lambda s: scenario_sort_key(
        s, (base.get(s) or cand.get(s) or {}).get("params")))
    lines = [f"## Comparison: `{candidate_label}` vs. baseline `{baseline_label}`", ""]
    b_env = baseline_report.get("environment", {})
    c_env = candidate_report.get("environment", {})
    if baseline_report is not candidate_report and (b_env.get("cpu"), b_env.get("cpus")) != (c_env.get("cpu"),
                                                                                           c_env.get("cpus")):
        lines += [f"> ⚠️ The reports come from different machines ({b_env.get('cpu')} x{b_env.get('cpus')} vs. "
                  f"{c_env.get('cpu')} x{c_env.get('cpus')}): differences may be due to the hardware.", ""]
    profile_differences = _differences(baseline_report.get("profile") or {}, candidate_report.get("profile") or {},
                                       PROFILE_CONDITIONS)
    if profile_differences:
        lines += ["> ⛔ The reports were run with different settings, so they measure different things and no metric "
                  "is judged better or worse: " + "; ".join(profile_differences) + ".", ""]
    lines += ["Changes within the noise tolerance of a metric are shown as ≈.", "",
              "| scenario | 🟢 better | 🔴 worse | worse metrics |", "|---|---:|---:|---|"]
    details = []
    total_worse = 0
    for scenario in scenarios:
        b, c = base.get(scenario), cand.get(scenario)
        problem = ("no baseline run" if not b else "no candidate run" if not c
                   else "baseline run failed" if not b["summary"] else "candidate run failed" if not c["summary"]
                   else None)
        if problem:
            lines.append(f"| {scenario} | – | – | ⚠️ {problem} |")
            continue
        # parameters both runs know (a parameter added to a later version of the benchmark keeps its old default)
        conditions = _differences(b["params"], c["params"], sorted(set(b["params"]) & set(c["params"])))
        comparable = not conditions and not profile_differences
        if conditions:
            lines.append(f"| {scenario} | – | – | ⛔ not comparable, different conditions: {'; '.join(conditions)} |")
        rows, better, worse = [], 0, []
        for metric in METRICS:
            bv, cv = b["summary"].get(metric.key), c["summary"].get(metric.key)
            if bv is None and cv is None:
                continue
            v, relative = verdict(metric, bv, cv)
            if not comparable and v in ("better", "worse", "same"):
                v = "n/a"
            better += v == "better"
            if v == "worse":
                worse.append(metric.title)
            change = f"{relative * 100:+.0f}%" if relative is not None else ""
            rows.append(f"| {metric.group} | {metric.title} | {fmt(bv, metric.unit)} | {fmt(cv, metric.unit)} | "
                        f"{change} | {SYMBOL[v]} |")
        total_worse += len(worse)
        if not conditions:
            lines.append(f"| {scenario} | {better} | {len(worse)} | {', '.join(worse)} |")
        details += [f"<details><summary><b>{scenario}</b>: {better} better, {len(worse)} worse</summary>", "",
                    f"| | metric | {baseline_label} | {candidate_label} | change | |", "|---|---|---:|---:|---:|---|",
                    *rows, "", "</details>", ""]
    return "\n".join(lines + [""] + details), total_worse


def compare_files(baseline: Path, candidate: Optional[Path], baseline_label: Optional[str],
                  candidate_label: Optional[str]) -> Tuple[str, int]:
    if baseline.is_dir():
        baseline = baseline / "report.json"
    base_report = json.loads(baseline.read_text())
    if candidate is None:
        cand_report = base_report
    else:
        if candidate.is_dir():
            candidate = candidate / "report.json"
        cand_report = json.loads(candidate.read_text())
    base_labels = list(base_report.get("labels", {})) or sorted({r["label"] for r in base_report["results"]})
    cand_labels = list(cand_report.get("labels", {})) or sorted({r["label"] for r in cand_report["results"]})
    if candidate is None:
        baseline_label = baseline_label or base_labels[0]
        candidate_label = candidate_label or next(label for label in base_labels if label != baseline_label)
    else:
        baseline_label = baseline_label or base_labels[0]
        candidate_label = candidate_label or cand_labels[-1]
    return render(base_report, baseline_label, cand_report, candidate_label)
