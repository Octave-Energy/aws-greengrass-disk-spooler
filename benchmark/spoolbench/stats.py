"""Small numeric helpers shared by the runner and the report."""

from typing import Dict, Iterable, List, Mapping, Optional

from .simulator import LATENCY_BUCKET_BASE


def merge_histograms(target: Dict[int, int], source: Mapping) -> Dict[int, int]:
    for bucket, count in (source or {}).items():
        target[int(bucket)] = target.get(int(bucket), 0) + int(count)
    return target


def histogram_percentiles_ms(hist: Mapping[int, int], percentiles: Iterable[float] = (50, 95, 99)) -> Dict[str, float]:
    """Percentiles of a log-bucket latency histogram (bucket i = [1.1^i, 1.1^(i+1)) µs), in milliseconds (±5%)."""
    total = sum(hist.values())
    result: Dict[str, Optional[float]] = {}
    if total == 0:
        for p in percentiles:
            result[f"p{int(p)}"] = None
        result["max"] = None
        return result
    ordered = sorted((int(b), c) for b, c in hist.items())
    for p in percentiles:
        threshold = total * p / 100.0
        seen = 0
        for bucket, count in ordered:
            seen += count
            if seen >= threshold:
                result[f"p{int(p)}"] = round(LATENCY_BUCKET_BASE ** (bucket + 0.5) / 1000.0, 3)
                break
    result["max"] = round(LATENCY_BUCKET_BASE ** (ordered[-1][0] + 1) / 1000.0, 3)
    return result


def mean(values: List[float]) -> Optional[float]:
    values = [v for v in values if v is not None]
    return sum(values) / len(values) if values else None
