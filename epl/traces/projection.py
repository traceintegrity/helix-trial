from __future__ import annotations

from typing import Any


SUPPORTED_BASELINES = {"raw", "gzip", "zstd"}


def project_storage_economics(
    *,
    daily_trace_volume: int,
    average_trace_size_bytes: int,
    baseline_method: str,
    epl_result_ratio: float,
) -> dict[str, Any]:
    if daily_trace_volume <= 0:
        raise ValueError("daily_trace_volume must be greater than zero")
    if average_trace_size_bytes <= 0:
        raise ValueError("average_trace_size_bytes must be greater than zero")
    if epl_result_ratio < 0:
        raise ValueError("epl_result_ratio must be zero or greater")
    if baseline_method not in SUPPORTED_BASELINES:
        raise ValueError(f"baseline_method must be one of {sorted(SUPPORTED_BASELINES)}")

    baseline_daily_bytes = daily_trace_volume * average_trace_size_bytes
    epl_daily_bytes = int(round(baseline_daily_bytes * epl_result_ratio))
    horizons = {}
    for days in (30, 90, 365):
        baseline_total = baseline_daily_bytes * days
        epl_total = epl_daily_bytes * days
        savings = baseline_total - epl_total
        horizons[f"{days}_days"] = {
            "days": days,
            "baseline_total_bytes": baseline_total,
            "epl_total_bytes": epl_total,
            "estimated_savings_bytes": savings,
            "estimated_savings_percent": round(savings / max(baseline_total, 1), 4),
        }

    return {
        "daily_trace_volume": daily_trace_volume,
        "average_trace_size_bytes": average_trace_size_bytes,
        "baseline_method": baseline_method,
        "baseline_size_assumption": "average_trace_size_bytes is interpreted as the average retained size per trace under the selected baseline method.",
        "epl_result_ratio": round(epl_result_ratio, 6),
        "baseline_daily_bytes": baseline_daily_bytes,
        "epl_daily_bytes": epl_daily_bytes,
        "horizons": horizons,
    }


__all__ = ["SUPPORTED_BASELINES", "project_storage_economics"]
