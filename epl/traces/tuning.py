from __future__ import annotations

from collections.abc import Sequence

from epl.traces.schema import TraceEvent, TraceSession, TraceSpan
from epl.traces.streaming import build_streaming_trace_packs


DEFAULT_RETENTION_CANDIDATES: tuple[int | None, ...] = (4, 8, 12, 16)


def parse_retention_candidates_spec(spec: str | None) -> tuple[int | None, ...]:
    if spec is None or not spec.strip():
        return DEFAULT_RETENTION_CANDIDATES
    raw_values = []
    for part in spec.split(","):
        stripped = part.strip().lower()
        if not stripped:
            continue
        if stripped in {"0", "none", "unbounded"}:
            raw_values.append(None)
            continue
        raw_values.append(int(stripped))
    if not raw_values:
        return DEFAULT_RETENTION_CANDIDATES
    return normalize_retention_candidates(raw_values)


def normalize_retention_candidates(
    candidate_caps: Sequence[int | None] | None,
    *,
    baseline_cap: int | None = None,
) -> tuple[int | None, ...]:
    raw_values = list(candidate_caps or DEFAULT_RETENTION_CANDIDATES)
    raw_values.append(baseline_cap)

    normalized: list[int | None] = []
    seen: set[int | None] = set()
    for value in raw_values:
        normalized_value = _normalize_candidate(value)
        if normalized_value in seen:
            continue
        seen.add(normalized_value)
        normalized.append(normalized_value)
    normalized.sort(key=_candidate_sort_key)
    return tuple(normalized)


def build_replayed_stream_sessions(
    sessions: Sequence[TraceSession],
    *,
    replay_passes: int,
) -> list[TraceSession]:
    if replay_passes <= 0:
        raise ValueError("replay_passes must be greater than zero")

    replayed_sessions: list[TraceSession] = []
    for pass_index in range(replay_passes):
        pass_label = f"p{pass_index + 1:03d}"
        for session in sessions:
            replayed_sessions.append(_clone_session_for_replay(session, pass_label))
    return replayed_sessions


def tune_stream_retention(
    sessions: Sequence[TraceSession],
    *,
    window_size: int,
    replay_passes: int = 4,
    candidate_caps: Sequence[int | None] | None = None,
    baseline_cap: int | None = None,
) -> dict[str, object]:
    if not sessions:
        raise ValueError("sessions must not be empty")

    normalized_candidates = normalize_retention_candidates(candidate_caps, baseline_cap=baseline_cap)
    stream_sessions = build_replayed_stream_sessions(sessions, replay_passes=replay_passes)
    candidate_rows: list[dict[str, object]] = []

    for candidate_cap in normalized_candidates:
        stream_result = build_streaming_trace_packs(
            stream_sessions,
            window_size=window_size,
            max_active_templates=candidate_cap,
        )
        stream_summary = stream_result["summary"]
        candidate_rows.append(
            {
                "candidate_max_active_templates": _display_candidate(candidate_cap),
                "candidate_type": "unbounded" if candidate_cap is None else "bounded",
                "is_baseline": int(candidate_cap == baseline_cap),
                "is_recommended": 0,
                "stream_replay_passes": replay_passes,
                "stream_replay_session_count": len(stream_sessions),
                "window_count": int(stream_summary["window_count"]),
                "stream_pack_total_bytes": int(stream_summary["stream_pack_total_bytes"]),
                "stream_windowed_raw_zlib_bytes": int(stream_summary["stream_windowed_raw_zlib_bytes"]),
                "stream_pack_gain_vs_windowed_raw_zlib": int(stream_summary["stream_pack_gain_vs_windowed_raw_zlib"]),
                "cross_window_template_reuse_rate": float(stream_summary["cross_window_template_reuse_rate"]),
                "final_active_template_count": int(stream_summary["final_active_template_count"]),
                "stream_new_template_total": int(stream_summary["stream_new_template_total"]),
                "stream_evicted_template_total": int(stream_summary["stream_evicted_template_total"]),
                "stream_reintroduced_template_total": int(stream_summary["stream_reintroduced_template_total"]),
                "gain_delta_vs_baseline": 0,
                "_candidate_cap": candidate_cap,
            }
        )

    baseline_row = next((row for row in candidate_rows if row["is_baseline"]), candidate_rows[0])
    recommended_row = sorted(candidate_rows, key=_recommendation_key, reverse=True)[0]
    baseline_gain = int(baseline_row["stream_pack_gain_vs_windowed_raw_zlib"])

    for row in candidate_rows:
        row["is_recommended"] = int(row is recommended_row)
        row["gain_delta_vs_baseline"] = int(row["stream_pack_gain_vs_windowed_raw_zlib"]) - baseline_gain

    summary = {
        "stream_replay_passes": replay_passes,
        "stream_replay_session_count": len(stream_sessions),
        "retention_tuning_candidate_count": len(candidate_rows),
        "baseline_max_active_templates": str(baseline_row["candidate_max_active_templates"]),
        "baseline_stream_pack_gain_vs_windowed_raw_zlib": baseline_gain,
        "recommended_max_active_templates": str(recommended_row["candidate_max_active_templates"]),
        "recommended_stream_pack_gain_vs_windowed_raw_zlib": int(recommended_row["stream_pack_gain_vs_windowed_raw_zlib"]),
        "recommended_cross_window_template_reuse_rate": float(recommended_row["cross_window_template_reuse_rate"]),
        "recommended_final_active_template_count": int(recommended_row["final_active_template_count"]),
        "recommended_gain_delta_vs_baseline": int(recommended_row["gain_delta_vs_baseline"]),
        "retention_recommendation_reason": "Highest stream gain, then lower active template count and lower churn.",
    }

    for row in candidate_rows:
        row.pop("_candidate_cap", None)

    return {
        "summary": summary,
        "candidate_rows": candidate_rows,
    }


def _clone_session_for_replay(session: TraceSession, pass_label: str) -> TraceSession:
    span_id_map = {
        span.span_id: f"{span.span_id}-{pass_label}"
        for span in session.spans
    }
    replayed_spans = []
    for span in session.spans:
        replayed_spans.append(
            TraceSpan(
                trace_id=f"{span.trace_id}-{pass_label}",
                span_id=span_id_map[span.span_id],
                parent_span_id=span_id_map.get(span.parent_span_id) if span.parent_span_id is not None else None,
                name=span.name,
                kind=span.kind,
                status=span.status,
                attributes=dict(span.attributes),
                events=tuple(TraceEvent(name=event.name, attributes=dict(event.attributes)) for event in span.events),
            )
        )
    return TraceSession(
        session_id=f"{session.session_id}-{pass_label}",
        workflow=session.workflow,
        spans=tuple(replayed_spans),
    )


def _normalize_candidate(value: int | None) -> int | None:
    if value in {None, 0}:
        return None
    normalized = int(value)
    if normalized <= 0:
        raise ValueError("retention candidate caps must be positive or zero for unbounded")
    return normalized


def _candidate_sort_key(value: int | None) -> tuple[int, int]:
    return (1 if value is None else 0, value if value is not None else 10**9)


def _display_candidate(value: int | None) -> str:
    return "unbounded" if value is None else str(value)


def _recommendation_key(row: dict[str, object]) -> tuple[int, int, int, int]:
    candidate_label = str(row["candidate_max_active_templates"])
    candidate_rank = 10**9 if candidate_label == "unbounded" else int(candidate_label)
    return (
        int(row["stream_pack_gain_vs_windowed_raw_zlib"]),
        -int(row["final_active_template_count"]),
        -int(row["stream_reintroduced_template_total"]),
        -candidate_rank,
    )
