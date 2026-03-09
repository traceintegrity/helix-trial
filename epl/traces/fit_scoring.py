from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from epl.traces.codec import benchmark_trace_codec
from epl.traces.corpus import discover_trace_sources
from epl.traces.fixtures import detect_trace_input_format, load_trace_sessions
from epl.traces.schema import TraceSession


VOLATILE_KEYS = {
    "session.id",
    "trace.id",
    "span.id",
    "parent.span.id",
}

PROMPT_KEYS = {
    "input.value",
    "trace.input",
    "prompt.template",
    "prompt.value",
}


def build_workload_fit_report(
    *,
    input_path: Path,
    archive_gain_vs_source_gzip: int,
    source_gzip_total_bytes: int,
    recommendation: str,
    replay_verified: bool,
    workload_label: str | None = None,
) -> dict[str, object]:
    sources = discover_trace_sources(input_path)
    sessions: list[TraceSession] = []
    for source in sources:
        sessions.extend(load_trace_sessions(source, input_format=detect_trace_input_format(source)))
    return build_workload_fit_report_from_sessions(
        sessions=sessions,
        archive_gain_vs_source_gzip=archive_gain_vs_source_gzip,
        source_gzip_total_bytes=source_gzip_total_bytes,
        recommendation=recommendation,
        replay_verified=replay_verified,
        workload_label=workload_label or Path(input_path).stem,
    )


def build_workload_fit_report_from_sessions(
    *,
    sessions: Sequence[TraceSession],
    archive_gain_vs_source_gzip: int,
    source_gzip_total_bytes: int,
    recommendation: str,
    replay_verified: bool,
    workload_label: str,
) -> dict[str, object]:
    benchmark = benchmark_trace_codec(sessions)
    summary = benchmark["summary"]
    prompt_shape_reuse_rate = _prompt_shape_reuse_rate(sessions)
    metadata_redundancy_rate = _metadata_redundancy_rate(sessions)
    tool_call_density = _tool_call_density(sessions)
    replay_value_indicator = _replay_value_indicator(sessions)
    branchy_session_ratio = _branchy_session_ratio(sessions)
    archive_gain_ratio = round(archive_gain_vs_source_gzip / max(source_gzip_total_bytes, 1), 4)

    raw_metrics = {
        "workload_label": workload_label,
        "session_count": int(summary["session_count"]),
        "span_count": int(summary["span_count"]),
        "avg_spans_per_session": float(summary["avg_spans_per_session"]),
        "template_count": int(summary["template_count"]),
        "template_reuse_rate": float(summary["template_reuse_rate"]),
        "prompt_shape_reuse_rate": prompt_shape_reuse_rate,
        "metadata_redundancy_rate": metadata_redundancy_rate,
        "tool_call_density": tool_call_density,
        "branchy_session_ratio": branchy_session_ratio,
        "replay_value_indicator": replay_value_indicator,
        "source_gzip_total_bytes": int(source_gzip_total_bytes),
        "archive_gain_vs_source_gzip": int(archive_gain_vs_source_gzip),
        "archive_gain_ratio_vs_source_gzip": archive_gain_ratio,
        "recommendation": recommendation,
        "replay_verified": int(replay_verified),
    }

    label, score, explanation = _classify_fit(raw_metrics)
    return {
        "fit_label": label,
        "fit_score": score,
        "workload_label": workload_label,
        "raw_metrics": raw_metrics,
        "explanation": explanation,
        "human_summary": _human_summary(label, explanation),
    }


def _classify_fit(metrics: Mapping[str, object]) -> tuple[str, int, list[str]]:
    if int(metrics["replay_verified"]) != 1:
        return "weak_fit", -5, ["Replay verification failed, so EPL is not a safe archive candidate for this workload."]

    score = 2
    explanation: list[str] = ["Replay verification passed, so EPL preserves the core exact-replay contract on this workload."]

    archive_gain = int(metrics["archive_gain_vs_source_gzip"])
    archive_gain_ratio = float(metrics["archive_gain_ratio_vs_source_gzip"])
    template_reuse_rate = float(metrics["template_reuse_rate"])
    prompt_shape_reuse_rate = float(metrics["prompt_shape_reuse_rate"])
    metadata_redundancy_rate = float(metrics["metadata_redundancy_rate"])
    tool_call_density = float(metrics["tool_call_density"])
    branchy_session_ratio = float(metrics["branchy_session_ratio"])
    replay_value_indicator = float(metrics["replay_value_indicator"])
    avg_spans_per_session = float(metrics["avg_spans_per_session"])
    source_gzip_total_bytes = int(metrics["source_gzip_total_bytes"])

    if archive_gain > 0:
        score += 2
        explanation.append(f"EPL beats source plus gzip by {archive_gain:+d} bytes on this workload.")
    elif archive_gain_ratio > -0.03:
        score += 1
        explanation.append("EPL is close to source plus gzip, so a narrow pilot may still make sense if replay or extraction value matters.")
    else:
        score -= 2
        explanation.append("EPL does not beat source plus gzip on raw archive bytes for this workload.")

    if template_reuse_rate >= 0.55:
        score += 2
        explanation.append("Template reuse is high, which is the strongest structural signal that EPL can keep paying off as the workload grows.")
    elif template_reuse_rate >= 0.35:
        score += 1
        explanation.append("Template reuse is moderate, which supports a narrow fit rather than a broad default recommendation.")
    else:
        score -= 1
        explanation.append("Template reuse is weak, so the trace shape is not repeating enough to favor EPL strongly.")

    if prompt_shape_reuse_rate >= 0.4:
        score += 1
        explanation.append("Prompt or input shapes repeat enough that the workload behaves like a stable application, not a one-off export stream.")
    if metadata_redundancy_rate >= 0.35:
        score += 1
        explanation.append("Metadata redundancy is high, which helps semantic archive packing beyond raw gzip alone.")
    if tool_call_density >= 0.15:
        score += 1
        explanation.append("Tool-call density is high enough that replay has operational value, not just storage value.")
    if branchy_session_ratio >= 0.2:
        score += 1
        explanation.append("Branching behavior appears often enough that structural replay and later diffing would be meaningful.")
    if replay_value_indicator >= 0.55:
        score += 1
        explanation.append("The workload shows strong replay-value indicators: multi-span sessions, tools, and LLM spans all show up consistently.")
    if avg_spans_per_session < 3:
        score -= 1
        explanation.append("Sessions are very small, so archive-side semantic packing has less room to outperform gzip.")
    if source_gzip_total_bytes < 2048:
        score -= 1
        explanation.append("The total export is small enough that fixed archive overhead matters more than compression efficiency.")

    recommendation = str(metrics["recommendation"])
    if recommendation == "pilot_now" and score >= 6:
        return "likely_fit", score, explanation
    if recommendation in {"pilot_now", "narrow_pilot"} and score >= 3:
        return "narrow_fit", score, explanation
    return "weak_fit", score, explanation


def _human_summary(label: str, explanation: Sequence[str]) -> str:
    if label == "likely_fit":
        return "This workload looks like a strong EPL candidate because it shows replay-safe structure plus enough repeated shape to beat gzip."
    if label == "narrow_fit":
        return "This workload is plausible for a focused pilot, but the evidence suggests EPL should be targeted to the strongest workflows rather than rolled out broadly."
    return "This workload does not yet look like a strong EPL candidate, either because it is too small, too variable, or too weak on archive economics."


def _prompt_shape_reuse_rate(sessions: Sequence[TraceSession]) -> float:
    shapes: list[str] = []
    for session in sessions:
        for span in session.spans:
            for key in PROMPT_KEYS:
                value = span.attributes.get(key)
                if value in {None, ""}:
                    continue
                shapes.append(_normalize_prompt_shape(str(value)))
    if not shapes:
        return 0.0
    return round(1.0 - (len(set(shapes)) / len(shapes)), 4)


def _metadata_redundancy_rate(sessions: Sequence[TraceSession]) -> float:
    pairs: list[str] = []
    for session in sessions:
        for span in session.spans:
            for key, value in sorted(span.attributes.items()):
                if key in VOLATILE_KEYS or key in PROMPT_KEYS or key.startswith("output."):
                    continue
                rendered = _render_value_for_redundancy(value)
                pairs.append(f"{key}={rendered}")
    if not pairs:
        return 0.0
    return round(1.0 - (len(set(pairs)) / len(pairs)), 4)


def _tool_call_density(sessions: Sequence[TraceSession]) -> float:
    total_spans = sum(len(session.spans) for session in sessions)
    if total_spans == 0:
        return 0.0
    tool_like = sum(
        1
        for session in sessions
        for span in session.spans
        if span.kind in {"TOOL", "RETRIEVER"} or span.name.startswith("tool.") or span.name.startswith("retriever.")
    )
    return round(tool_like / total_spans, 4)


def _branchy_session_ratio(sessions: Sequence[TraceSession]) -> float:
    if not sessions:
        return 0.0
    branchy_count = 0
    for session in sessions:
        child_counts = Counter(span.parent_span_id for span in session.spans if span.parent_span_id is not None)
        if any(count > 1 for count in child_counts.values()):
            branchy_count += 1
    return round(branchy_count / len(sessions), 4)


def _replay_value_indicator(sessions: Sequence[TraceSession]) -> float:
    if not sessions:
        return 0.0
    session_scores: list[float] = []
    for session in sessions:
        span_count = len(session.spans)
        has_llm = any(span.kind == "LLM" for span in session.spans)
        has_tool = any(span.kind in {"TOOL", "RETRIEVER"} for span in session.spans)
        has_branch = _branchy_session_ratio((session,)) > 0.0
        score = 0.0
        if span_count >= 4:
            score += 0.4
        if has_llm:
            score += 0.2
        if has_tool:
            score += 0.2
        if has_branch:
            score += 0.2
        session_scores.append(min(score, 1.0))
    return round(sum(session_scores) / len(session_scores), 4)


def _normalize_prompt_shape(value: str) -> str:
    normalized = value.lower().strip()
    normalized = re.sub(r"[0-9]+", "<n>", normalized)
    normalized = re.sub(r"[a-f0-9]{8,}", "<hex>", normalized)
    normalized = re.sub(r"'[^']+'", "'<slot>'", normalized)
    normalized = re.sub(r'"[^"]+"', '"<slot>"', normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def _render_value_for_redundancy(value: Any) -> str:
    if isinstance(value, str):
        return _normalize_prompt_shape(value)
    if isinstance(value, (int, float, bool)) or value is None:
        return str(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


__all__ = [
    "build_workload_fit_report",
    "build_workload_fit_report_from_sessions",
]
