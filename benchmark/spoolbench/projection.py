"""
Disk needed for longer internet outages, extrapolated from the outage of a scenario.

During an outage nothing is delivered, so the backlog grows with the payload IBEMS publishes. The spooler keeps the
first `diskSpillThresholdBytes` of it in RAM and writes the rest to spooler.db, which grows by a fairly constant number
of disk bytes per payload byte (SQLite pages, row and index overhead). For an outage of duration D:

    payload(D) = payload rate × D
    disk(D)    = disk bytes per payload byte × max(0, payload(D) − diskSpillThresholdBytes)

The Nucleus caps the spool at `mqtt.spooler.maxSizeInBytes` of payload: beyond that it rejects new publishes
("Message spool is full") and the database stops growing, so the disk needed to keep *every* message of a longer outage
also needs a larger maxSizeInBytes.
"""

from typing import Any, Dict, List, Optional, Tuple

HOUR = 3600
DURATIONS: List[Tuple[str, float]] = [("1 h", HOUR), ("6 h", 6 * HOUR), ("12 h", 12 * HOUR), ("1 d", 24 * HOUR),
                                      ("7 d", 7 * 24 * HOUR)]
NUCLEUS_DEFAULT_SPOOL_BYTES = 2_621_440  # mqtt.spooler.maxSizeInBytes when not configured (2.5 MiB)
# below this much payload on disk, the measured overhead is dominated by the database's fixed size and free pages
MIN_DISK_PAYLOAD_BYTES = 256 * 1024


def spool_limit_bytes(profile: Dict[str, Any]) -> int:
    spooler = (profile.get("nucleus_configuration") or {}).get("mqtt", {}).get("spooler", {})
    return int(spooler.get("maxSizeInBytes", NUCLEUS_DEFAULT_SPOOL_BYTES))


def measure(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Payload rate and disk bytes per payload byte during the outage, or None if the result cannot tell.

    Not for device-crash scenarios: the crash empties the RAM part of the spool and the reboot interrupts the
    publishing, so their outage is not a steady state to extrapolate from."""
    params = result.get("scenario_params") or {}
    outage = (result.get("phases") or {}).get("outage") or {}
    if params.get("device_crash") or params.get("power_loss") or result.get("status") not in (None, "ok"):
        return None
    duration, payload = outage.get("duration_s"), outage.get("accepted_bytes")
    db_start, db_end = outage.get("spool_db_bytes_start"), outage.get("spool_db_bytes_end")
    if not duration or not payload or db_start is None or db_end is None:
        return None
    threshold = params.get("spill_threshold_bytes") or 0
    on_disk = payload - threshold  # the backlog beyond the spill threshold went to disk
    return {
        "msg_rate": outage.get("accepted", 0) / duration,
        "payload_rate_bytes_s": payload / duration,
        "spill_threshold_bytes": threshold,
        "disk_per_payload": (db_end - db_start) / on_disk if on_disk >= MIN_DISK_PAYLOAD_BYTES else None,
    }


def project(results: List[Dict[str, Any]], profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    """One projection per result that can be extrapolated, in the order of the results."""
    limit = spool_limit_bytes(profile)
    measured = [(r, measure(r)) for r in results]
    projections = []
    for result, m in measured:
        if m is None:
            continue
        ratio, source = m["disk_per_payload"], None
        if ratio is None:
            # too little went to disk (short outage, low rate, large spill threshold): borrow the overhead of the same
            # build's scenario that wrote the most to disk
            donors = [(r, d) for r, d in measured if d and d["disk_per_payload"] is not None
                      and r["label"] == result["label"]]
            if not donors:
                continue
            donor, d = max(donors, key=lambda rd: rd[0]["phases"]["outage"]["accepted_bytes"]
                           - rd[1]["spill_threshold_bytes"])
            ratio, source = d["disk_per_payload"], donor["scenario"]
        rate, threshold = m["payload_rate_bytes_s"], m["spill_threshold_bytes"]
        projections.append({
            "scenario": result["scenario"],
            "label": result["label"],
            "msg_rate": m["msg_rate"],
            "payload_rate_bytes_s": rate,
            "disk_per_payload": ratio,
            "disk_per_payload_from": source,
            "disk_bytes_per_hour": ratio * rate * HOUR,
            "durations": {name: {"payload_bytes": rate * seconds,
                                 "disk_bytes": ratio * max(0.0, rate * seconds - threshold),
                                 "over_spool_limit": rate * seconds > limit}
                          for name, seconds in DURATIONS},
            "spool_limit_bytes": limit,
            "spool_full_after_s": limit / rate,
            "disk_at_spool_limit_bytes": ratio * max(0.0, limit - threshold),
        })
    return projections
