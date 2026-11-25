"""
Builds the report of a results directory (<out>/<label>/<scenario>/result.json + timeseries.csv, <out>/meta.json):

- report.json   machine-readable, the input of `compare` (schema "spoolbench-report/1")
- report.md     key results per scenario and build, comparison of the first two builds, phase details
- summary.md    report.md with Mermaid charts instead of images, for the GitHub job summary
- charts/*.png  time series per scenario, every build overlaid, aligned on the start of the outage
"""

import csv
import datetime
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import projection
from .compare import METRICS_BY_KEY, MIN_CAPACITY_BACKLOG_MSGS, fmt, render, scenario_sort_key

SCHEMA = "spoolbench-report/1"
# reference categorical palette (validated for colour vision deficiencies), assigned in fixed order to the builds
SERIES_COLOURS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
# the closest coloured square per series colour: Mermaid charts have no legend, the job summary shows these instead
SERIES_SQUARES = ["🟦", "🟧", "🟩", "🟨"]
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
    ("recovery_first_delivery_s", "reconnect"),
    ("recovery_drain_after_first_delivery_s", "drain"),
    ("recovery_drain_rate_after_first_delivery_msg_s", "drain msg/s"),
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


# charts of the job summary (GitHub renders Mermaid, not images there): (title, column, scale, per second)
MERMAID_CHARTS = [
    ("backlog (messages)", "backlog", 1.0, False),
    ("delivered to the cloud (msg/s)", "delivered", 1.0, True),
    ("spooler.db (MiB)", "spool_db_bytes", 1024 * 1024, False),
    ("JVM heap used (MiB)", "heap_used_bytes", 1024 * 1024, False),
]
MERMAID_POINTS = 60


def _legend_entry(index: int, label: str, ref: Optional[str]) -> str:
    """'🟦 **baseline** `main-octave@4a0f945`': the build in the colour of its chart line (index in SERIES_COLOURS)."""
    square = SERIES_SQUARES[index] if index < len(SERIES_SQUARES) else f"`{SERIES_COLOURS[index % len(SERIES_COLOURS)]}`"
    return f"{square} **{label}**" + (f" `{ref}`" if ref else "")


def _mermaid_charts(scenario: str, runs: Dict[str, Path], colours: Dict[str, str],
                    refs: Dict[str, str]) -> List[str]:
    """The time series as Mermaid line charts, every build resampled onto one time grid (outage starts at 0).

    Mermaid's xychart has no legend: every chart gets a line above it with the colour of each build."""
    series: Dict[str, List[Dict[str, Any]]] = {}
    for label, directory in runs.items():
        rows = _read_timeseries(directory / "timeseries.csv")
        if rows:
            start = next((r["t"] for r in rows if r["phase"] == "outage"), rows[0]["t"])
            series[label] = [{**r, "t": r["t"] - start} for r in rows]
    if not series:
        return []
    t_min = min(rows[0]["t"] for rows in series.values())
    t_max = max(rows[-1]["t"] for rows in series.values())
    step = max((t_max - t_min) / (MERMAID_POINTS - 1), 1.0)
    grid = [t_min + i * step for i in range(int((t_max - t_min) / step) + 1)]

    def sample(rows, key, scale, per_second):
        values, j = [], 0
        for t in grid:
            while j + 1 < len(rows) and rows[j + 1]["t"] <= t:
                j += 1
            cur = rows[j]
            value = cur.get(key)
            if per_second:
                prev = next((r for r in reversed(rows[:j]) if r["t"] <= cur["t"] - step), rows[0])
                dt = cur["t"] - prev["t"]
                value = (cur[key] - prev[key]) / dt if dt > 0 and value is not None and prev.get(key) is not None \
                    else 0.0
            values.append(round((value or 0.0) / scale, 2))
        return values

    # colours follow the build, as in the PNG charts, whichever builds this scenario has
    labels = [label for label in colours if label in series]
    legend = " · ".join(_legend_entry(list(colours).index(label), label, refs.get(label)) for label in labels)
    lines = []
    palette = ", ".join(colours[label] for label in labels)
    for title, key, scale, per_second in MERMAID_CHARTS:
        data = {label: sample(rows, key, scale, per_second) for label, rows in series.items()}
        if not any(any(v) for v in data.values()):
            continue
        lines += [f"Lines: {legend}", "", "```mermaid", "---", "config:", "  xyChart:", "    width: 700", "    height: 260",
                  "  themeVariables:", "    xyChart:", f'      plotColorPalette: "{palette}"', "---", "xychart-beta",
                  f'  title "{scenario}: {title}"',
                  f"  x-axis \"seconds since the outage started\" {round(grid[0])} --> {round(grid[-1])}",
                  f'  y-axis "{title}"']
        lines += [f"  line [{', '.join(f'{v:g}' for v in data[label])}]" for label in labels]
        lines += ["```", ""]
    return lines


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


def _scenario_types(results: List[Dict[str, Any]]) -> List[str]:
    """Explains the kinds of scenario in this report, and only those (the quick profile has no reset, for example)."""
    params = [r["scenario_params"] for r in results]
    rows = [("`r<rate>-spill<threshold>-outage`", "**internet outage**: the link to AWS IoT Core is down, the device "
             "keeps running and the spooler takes over")]
    if any(p.get("device_crash") or p.get("power_loss") for p in params):
        rows.append(("`…-outage-devicecrash`", "internet outage, and the whole device crashes during it and boots "
                     "again while still offline"))
    if any(p.get("outage_mode") == "reset" for p in params):
        rows.append(("`…-outage-reset`", "internet outage where the connections are reset instead of black-holed"))
    return (["Every scenario cuts the internet while IBEMS keeps publishing:", "",
             "| scenario | what happens |", "|---|---|"]
            + [f"| {name} | {text} |" for name, text in rows]
            + ["", "`r<rate>`: IBEMS publish rate in msg/s. `spill<threshold>`: diskSpillThresholdBytes, the backlog "
                   "kept in RAM before the spooler writes to disk (`spill0`: disk only; `spill300k`: production)."])


KEY_COLUMN_GROUPS = [
    ("Online (link up)", [
        ("accepted msg/s", "publishes the Nucleus accepted per second"),
        ("delivered msg/s", "messages received in the cloud per second"),
        ("e2e p50", "from creation of the batch to reception in the cloud"),
        ("IPC p99", "time IBEMS waits for a publish to be accepted"),
    ]),
    ("Outage (link down)", [
        ("spooled payload", "payload bytes accepted during the outage"),
        ("db growth", "growth of spooler.db (+ journal) during the outage"),
        ("disk B / payload B", "db growth per spooled payload byte"),
        ("db after drain", "size of spooler.db at the end; SQLite does not give space back"),
        ("heap for backlog", "live heap (after a full GC) at the end of the outage minus online"),
        ("RSS peak", "peak resident memory of the Nucleus"),
    ]),
    ("Recovery (link back up)", [
        ("backlog at link up", "messages waiting in the spooler when the internet came back"),
        ("reconnect", "from the link coming back to the first delivery. The Nucleus MQTT client retries with growing "
                      "pauses, so this depends on when the link came back relative to the next attempt, not on the "
                      "spooler. It is shown but not compared, and *drain* starts after it"),
        ("drain", "time to deliver that backlog while IBEMS keeps publishing, from the first delivery"),
        ("drain msg/s", "backlog / drain time. Messages are delivered in order, so this is close to the delivery "
                        "capacity. After an outage shorter than the MQTT keep-alive the Nucleus has not noticed it "
                        "and part of the backlog arrives at once from the stalled connection, so small backlogs drain "
                        "faster"),
        ("capacity msg/s", "delivery rate while working through a backlog with no new messages (only measured on a "
                           f"backlog of at least {MIN_CAPACITY_BACKLOG_MSGS:,} messages)"),
    ]),
    ("Integrity", [
        ("lost", "accepted by the Nucleus but never delivered"),
        ("dup", "delivered more than once"),
        ("rejected", "publishes the Nucleus refused"),
    ]),
]


def _column_definitions() -> List[str]:
    lines = ["<details><summary>What the columns mean</summary>", ""]
    for group, columns in KEY_COLUMN_GROUPS:
        lines += [f"**{group}**", ""] + [f"- *{name}*: {text}" for name, text in columns] + [""]
    return lines + ["</details>"]


def _duration(seconds: float) -> str:
    if seconds < 3600:
        return f"{seconds / 60:,.0f} min" if seconds >= 600 else f"{seconds / 60:,.1f} min"
    return f"{seconds / 3600:,.1f} h" if seconds < 48 * 3600 else f"{seconds / 86400:,.1f} d"


def _projection_section(projections: List[Dict[str, Any]], profile: Dict[str, Any]) -> List[str]:
    limit = projection.spool_limit_bytes(profile)
    names = [name for name, _ in projection.DURATIONS]
    lines = ["## Disk needed for longer outages", "",
             "Extrapolated from the outage of each scenario: while the internet is down nothing is delivered, so "
             "spooler.db grows with the payload IBEMS publishes. *disk* = *disk B / payload B* × (*payload/s* × "
             "outage duration − diskSpillThresholdBytes): the size spooler.db needs to keep **every** message of an "
             "outage that long at that rate.", "",
             f"⛔ The Nucleus caps the spool at `maxSizeInBytes` = {fmt(limit, 'bytes')} of payload. Once it is full "
             "(*spool full after*) it rejects new publishes (*Message spool is full*): the database stops growing at "
             "*db at spool limit* and the messages after that are lost. Keeping them needs a larger `maxSizeInBytes` "
             "as well as the disk. Device-crash scenarios are not extrapolated.", "",
             "| scenario | build | outage msg/s | payload/s | disk B / payload B | disk / hour | "
             + " | ".join(names) + " | spool full after | db at spool limit |",
             "|---|---|" + "---:|" * (6 + len(names))]
    borrowed = False
    for p in projections:
        ratio = fmt(p["disk_per_payload"], "ratio")
        if p["disk_per_payload_from"]:
            ratio += " ¹"
            borrowed = True
        cells = [fmt(p["msg_rate"], "rate"), fmt(p["payload_rate_bytes_s"], "bytes"), ratio,
                 fmt(p["disk_bytes_per_hour"], "bytes")]
        cells += [fmt(d["disk_bytes"], "bytes") + (" ⛔" if d["over_spool_limit"] else "")
                  for d in p["durations"].values()]
        cells += [_duration(p["spool_full_after_s"]), fmt(p["disk_at_spool_limit_bytes"], "bytes")]
        lines.append(f"| {p['scenario']} | {p['label']} | " + " | ".join(cells) + " |")
    if not projections:
        lines.append("| no scenario to extrapolate from |")
    if borrowed:
        lines += ["", "¹ too little of this outage went to disk (most of it stayed in RAM below the spill threshold): "
                      "disk B / payload B of the same build's scenario that wrote the most to disk."]
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
    projections = projection.project(results, meta.get("profile", {}))
    report["disk_projection"] = projections
    (out / "report.json").write_text(json.dumps(report, indent=2, default=str))

    colours = {label: SERIES_COLOURS[i % len(SERIES_COLOURS)] for i, label in enumerate(labels)}
    refs = {label: report["labels"][label].get("ref") for label in labels}
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
              *_scenario_types(results), ""]
    lines += _key_table(results, labels)
    lines += [""] + _column_definitions() + [""]
    lines += _projection_section(projections, profile) + [""]

    if len(labels) >= 2:
        text, _ = render(report, labels[0], report, labels[1])
        lines += [text, ""]

    lines += ["## Scenarios", ""]
    summary_charts: Dict[int, List[str]] = {}  # line of a chart image in report.md -> its Mermaid version
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
            summary_charts[len(lines) - 2] = _mermaid_charts(scenario, runs, colours, refs)
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
    # the same report for the GitHub job summary, which shows Mermaid charts but no images
    summary: List[str] = []
    for index, line in enumerate(lines):
        summary.extend(summary_charts.get(index, [line]))
    (out / "summary.md").write_text("\n".join(summary))
    print(f"report: {out / 'report.md'}")
    return out / "report.md"
