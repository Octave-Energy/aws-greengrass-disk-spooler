"""
Builds the report of a results directory (<out>/<label>/<scenario>/result.json + timeseries.csv, <out>/meta.json):

- report.json   machine-readable, the input of `compare` (schema "spoolbench-report/1")
- report.md     key results per scenario and build, comparison of the first two builds, phase details
- charts/*.png  time series per scenario, every build overlaid, aligned on the start of the outage
"""

import csv
import datetime
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from .compare import METRICS_BY_KEY, fmt, render, scenario_sort_key

SCHEMA = "spoolbench-report/1"
# reference categorical palette (validated for colour vision deficiencies), assigned in fixed order to the builds
SERIES_COLOURS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
OUTAGE_PHASES = ("outage", "device_crash", "offline_after_reboot")

# columns of the key results table: (summary key, short header)
KEY_COLUMNS = [
    ("ingest_rate_msg_s", "accepted msg/s"),
    ("delivery_rate_online_msg_s", "delivered msg/s"),
    ("e2e_latency_p50_ms", "e2e p50"),
    ("ipc_latency_p99_ms", "IPC p99"),
    ("outage_accepted_payload_bytes", "spooled payload"),
    ("disk_db_growth_outage_bytes", "db growth"),
    ("disk_bytes_per_payload_byte", "disk B / payload B"),
    ("disk_db_bytes_after_stop", "db after drain"),
    ("heap_backlog_bytes", "heap for backlog"),
    ("rss_peak_bytes", "RSS peak"),
    ("recovery_backlog_msgs", "backlog at link up"),
    ("recovery_drain_s", "drain"),
    ("recovery_drain_rate_msg_s", "drain msg/s"),
    ("cooldown_delivery_rate_msg_s", "capacity msg/s"),
    ("lost_msgs", "lost"),
    ("duplicate_msgs", "dup"),
    ("rejected_msgs", "rejected"),
]


def _load_results(out: Path) -> List[Dict[str, Any]]:
    results = []
    for path in sorted(out.glob("*/*/result.json")):
        result = json.loads(path.read_text())
        result["scenario_params"] = result.pop("scenario")
        result["scenario"] = path.parent.name.split("-rep")[0]
        result["directory"] = str(path.parent.relative_to(out))
        result.get("events", {}).pop("lost_seqs", None)
        results.append(result)
    # in the order of the workload (10, 50, 100 msg/s, ...) rather than alphabetically
    results.sort(key=lambda r: (scenario_sort_key(r["scenario"], r["scenario_params"]), r["label"], r.get("repeat", 1)))
    return results


def _read_timeseries(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    with open(path) as f:
        for row in csv.DictReader(f):
            parsed: Dict[str, Any] = {}
            for key, value in row.items():
                if key in ("phase", "link"):
                    parsed[key] = value
                else:
                    try:
                        parsed[key] = float(value) if value != "" else None
                    except ValueError:
                        parsed[key] = None
            rows.append(parsed)
    return rows


def _chart(out: Path, scenario: str, runs: Dict[str, Path], colours: Dict[str, str]) -> Optional[Path]:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return None
    panels = [("throughput", "messages / s"), ("backlog", "backlog (messages)"), ("db", "spooler.db (MiB)"),
              ("heap", "JVM heap used (MiB)"), ("rss", "Nucleus RSS (MiB)")]
    fig, axes = plt.subplots(len(panels), 1, figsize=(10, 12), sharex=True)
    outage_span = None
    has_crash = False
    for label, directory in runs.items():
        rows = _read_timeseries(directory / "timeseries.csv")
        if not rows:
            continue
        start = next((r["t"] for r in rows if r["phase"] == "outage"), rows[0]["t"])
        recovery = next((r["t"] for r in rows if r["phase"] == "recovery"), None)
        if outage_span is None and recovery is not None:
            outage_span = (0.0, recovery - start)
        t = [r["t"] - start for r in rows]
        colour = colours[label]
        crash = next((r["t"] - start for r in rows if r["phase"] == "device_crash"), None)
        if crash is not None:
            has_crash = True
            for ax in axes:
                ax.axvline(crash, color=colour, linewidth=1, linestyle="--", alpha=0.7)

        def series(key, scale=1.0):
            return [r[key] / scale if r.get(key) is not None else float("nan") for r in rows]

        def per_second(key, window=3):
            # over a few samples: the simulator reports its counters once a second, with some jitter
            values = []
            for i, cur in enumerate(rows):
                prev = rows[max(i - window, 0)]
                dt = cur["t"] - prev["t"]
                ok = dt > 0 and cur.get(key) is not None and prev.get(key) is not None
                values.append((cur[key] - prev[key]) / dt if ok else float("nan"))
            return values

        axes[0].plot(t, per_second("accepted"), color=colour, linewidth=1.6, label=f"{label} accepted")
        axes[0].plot(t, per_second("delivered"), color=colour, linewidth=1.6, linestyle=":",
                     label=f"{label} delivered")
        axes[1].plot(t, series("backlog"), color=colour, linewidth=2, label=label)
        axes[2].plot(t, series("spool_db_bytes", 1024 * 1024), color=colour, linewidth=2, label=label)
        axes[3].plot(t, series("heap_used_bytes", 1024 * 1024), color=colour, linewidth=2, label=label)
        axes[4].plot(t, series("rss_bytes", 1024 * 1024), color=colour, linewidth=2, label=label)
    for ax, (_, title) in zip(axes, panels):
        ax.set_ylabel(title, fontsize=9, color="#52514e")
        ax.grid(True, color="#e6e5e0", linewidth=0.8)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color("#c3c2b7")
        ax.tick_params(colors="#52514e", labelsize=8)
        if outage_span:
            ax.axvspan(*outage_span, color="#52514e", alpha=0.08, linewidth=0)
    axes[0].legend(fontsize=8, ncol=2, frameon=False, loc="upper right")
    axes[1].legend(fontsize=8, frameon=False, loc="upper right")
    if outage_span:
        axes[0].annotate("internet outage (dashed line: device crash)" if has_crash else "internet outage",
                         xy=(outage_span[0], 1), xycoords=("data", "axes fraction"), fontsize=8, color="#52514e",
                         va="bottom")
    axes[-1].set_xlabel("seconds since the start of the outage", fontsize=9, color="#52514e")
    fig.suptitle(scenario, fontsize=12, x=0.06, ha="left", color="#0b0b0b")
    fig.tight_layout()
    charts = out / "charts"
    charts.mkdir(exist_ok=True)
    path = charts / f"{scenario}.png"
    fig.savefig(path, dpi=110)
    plt.close(fig)
    return path


def _key_table(results: List[Dict[str, Any]], labels: List[str]) -> List[str]:
    lines = ["| scenario | build | " + " | ".join(h for _, h in KEY_COLUMNS) + " |",
             "|---|---|" + "---:|" * len(KEY_COLUMNS)]
    from .compare import aggregate

    by_label = {label: aggregate([r for r in results if r["label"] == label]) for label in labels}
    scenarios: List[str] = []
    for r in results:
        if r["scenario"] not in scenarios:
            scenarios.append(r["scenario"])
    for scenario in scenarios:
        for label in labels:
            entry = by_label[label].get(scenario)
            if not entry:
                continue
            if not entry["summary"]:
                lines.append(f"| {scenario} | {label} | failed: {'; '.join(map(str, entry['errors']))} |")
                continue
            cells = [fmt(entry["summary"].get(key), METRICS_BY_KEY[key].unit) for key, _ in KEY_COLUMNS]
            lines.append(f"| {scenario} | {label} | " + " | ".join(cells) + " |")
    return lines


def _phase_table(result: Dict[str, Any]) -> List[str]:
    lines = ["| phase | duration | accepted | rejected | delivered | accepted/s | delivered/s | IPC p99 | e2e p50 | "
             "CPU | backlog end | db end | RSS end |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for phase, m in result.get("phases", {}).items():
        lines.append("| " + " | ".join([
            phase, fmt(m.get("duration_s"), "s"), fmt(m.get("accepted"), "count"), fmt(m.get("rejected"), "count"),
            fmt(m.get("delivered"), "count"), fmt(m.get("accepted_rate"), "rate"), fmt(m.get("delivered_rate"), "rate"),
            fmt(m.get("ipc_latency_ms", {}).get("p99"), "ms"), fmt(m.get("e2e_latency_ms", {}).get("p50"), "ms"),
            fmt(m.get("cpu_pct"), "pct"), fmt(m.get("backlog_end"), "count"), fmt(m.get("spool_db_bytes_end"), "bytes"),
            fmt(m.get("rss_bytes_end"), "bytes")]) + " |")
    return lines


def _checkpoint_table(result: Dict[str, Any]) -> List[str]:
    lines = ["| checkpoint | live heap | SpoolMessage objects | byte[] | spooler.db | RSS |",
             "|---|---:|---:|---:|---:|---:|"]
    for name, c in result.get("checkpoints", {}).items():
        lines.append(f"| {name} | {fmt(c.get('live_heap_bytes'), 'bytes')} | "
                     f"{fmt(c.get('spool_message_instances'), 'count')} | {fmt(c.get('byte_array_bytes'), 'bytes')} | "
                     f"{fmt(c.get('spool_db_bytes'), 'bytes')} | {fmt(c.get('rss_bytes'), 'bytes')} |")
    return lines


def build(out: Path) -> Path:
    meta = json.loads((out / "meta.json").read_text()) if (out / "meta.json").exists() else {}
    results = _load_results(out)
    labels = list(meta.get("labels", {}))
    for r in results:
        if r["label"] not in labels:
            labels.append(r["label"])
    report = {"schema": SCHEMA, "generated": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
              **meta, "labels": {label: meta.get("labels", {}).get(label, {}) for label in labels},
              "results": results}
    (out / "report.json").write_text(json.dumps(report, indent=2, default=str))

    colours = {label: SERIES_COLOURS[i % len(SERIES_COLOURS)] for i, label in enumerate(labels)}
    env = meta.get("environment", {})
    profile = meta.get("profile", {})
    payloads = meta.get("payloads", {})
    lines = ["# Disk spooler benchmark", ""]
    lines += ["| build | ref |", "|---|---|"]
    lines += [f"| {label} | {report['labels'][label].get('ref') or '–'} |" for label in labels]
    lines += ["", f"Profile `{meta.get('profile_file', '?')}`: Nucleus {profile.get('nucleus_version')}, spooler "
              f"`{json.dumps(profile.get('nucleus_configuration', {}).get('mqtt', {}).get('spooler', {}))}`, "
              f"{profile.get('max_outstanding')} publish(es) in flight, payload seed {profile.get('seed')}.",
              f"Machine: {env.get('cpu')} x{env.get('cpus')}, {fmt(env.get('memory_bytes'), 'bytes')} RAM, "
              f"{env.get('os')}, {env.get('java')}." + (f" [Run]({env['github_run']})" if env.get("github_run") else ""),
              f"Payloads (IBEMS MeasurementMessage, protobuf): mean {fmt(payloads.get('mean_bytes'), 'bytes')}, p50 "
              f"{fmt(payloads.get('p50_bytes'), 'bytes')}, p95 {fmt(payloads.get('p95_bytes'), 'bytes')}, max "
              f"{fmt(payloads.get('max_bytes'), 'bytes')}.", ""]

    lines += ["## Key results", "",
              "Every scenario has an **internet outage**: `…-outage` the link to AWS IoT Core is down while the device "
              "keeps running and the spooler takes over; `…-outage-devicecrash` the whole device also crashes during "
              "the outage and boots again while still offline; `…-outage-reset` the connections are reset instead of "
              "black-holed. `r<rate>` is the publish rate in msg/s, `spill<threshold>` the diskSpillThresholdBytes "
              "(spill0: disk only, no RAM).", "",
              "Online: steady state with the link up. Outage: the internet is down and the spooler takes over. "
              "*e2e*: from creation of the batch to reception in the cloud. *db growth*: spooler.db (+journal) growth "
              "during the outage; *disk B / payload B*: that growth per spooled payload byte. *heap for backlog*: live "
              "heap (after a full GC) at the end of the outage minus online. *backlog at link up*: messages waiting "
              "in the spooler when the internet came back. *drain*: time to deliver that backlog while IBEMS keeps "
              "publishing; *drain msg/s*: backlog / drain time; messages are delivered in order, so new ones queue behind the "
              "backlog and this is close to the delivery capacity. After an outage shorter than the MQTT keep-alive "
              "the Nucleus has not noticed it and part of the backlog arrives at once from the stalled connection, so "
              "small backlogs drain faster than the capacity. *capacity*: delivery rate when working through a backlog with no "
              "new messages. *lost*: accepted by the Nucleus but never delivered.", ""]
    lines += _key_table(results, labels)
    lines.append("")

    if len(labels) >= 2:
        text, _ = render(report, labels[0], report, labels[1])
        lines += [text, ""]

    lines += ["## Scenarios", ""]
    scenarios: List[str] = []
    for r in results:
        if r["scenario"] not in scenarios:
            scenarios.append(r["scenario"])
    for scenario in scenarios:
        runs = {r["label"]: out / r["directory"] for r in results if r["scenario"] == scenario}
        chart = _chart(out, scenario, runs, colours)
        params = next(r["scenario_params"] for r in results if r["scenario"] == scenario)
        lines += [f"### {scenario}", "",
                  f"{params['rate']:g} msg/s, diskSpillThresholdBytes {params['spill_threshold_bytes']}, online "
                  f"{params['online_s']:g} s, outage {params['outage_s']:g} s ({params['outage_mode']})"
                  + (f", device crash during the outage and {params['offline_after_reboot_s']:g} s offline after the reboot"
                     if params.get("device_crash") else "") + ".", ""]
        if chart:
            lines += [f"![{scenario}](charts/{chart.name})", ""]
        for r in (r for r in results if r["scenario"] == scenario):
            title = f"{r['label']}" + (f" (repeat {r.get('repeat')})" if r.get("repeat", 1) != 1 else "")
            lines += [f"<details><summary>{title}: phases, memory checkpoints, log events</summary>", ""]
            if r.get("status") != "ok":
                lines += [f"**Failed:** `{r.get('error')}`", ""]
            lines += _phase_table(r) + [""] + _checkpoint_table(r) + [""]
            log = r.get("log") or {}
            if log.get("top"):
                lines += [f"WARN: {log.get('warn')}, ERROR: {log.get('error')}", "", "| count | event |", "|---:|---|"]
                lines += [f"| {e['count']} | `{e['event'].replace('|', '/')}` |" for e in log["top"]]
                lines.append("")
            lines += ["</details>", ""]
    (out / "report.md").write_text("\n".join(lines))
    print(f"report: {out / 'report.md'}")
    return out / "report.md"
