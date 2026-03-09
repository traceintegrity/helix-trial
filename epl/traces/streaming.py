from __future__ import annotations

import json
import zlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from epl.traces.codec import SemanticTraceCodec, TraceTemplate, canonicalize_session
from epl.traces.schema import TraceSession


@dataclass(frozen=True, slots=True)
class StreamingWindowPack:
    window_index: int
    session_start_index: int
    session_end_index: int
    manifest: dict[str, object]
    compressed_payload: bytes


def build_streaming_trace_packs(
    sessions: Sequence[TraceSession],
    *,
    window_size: int,
    max_active_templates: int | None = None,
) -> dict[str, object]:
    if not sessions:
        raise ValueError("sessions must not be empty")
    if window_size <= 0:
        raise ValueError("window_size must be greater than zero")

    codec = SemanticTraceCodec()
    window_packs: list[StreamingWindowPack] = []
    window_rows: list[dict[str, object]] = []
    payloads: list[dict[str, object]] = []

    total_stream_pack_bytes = 0
    total_windowed_raw_zlib_bytes = 0
    total_windowed_raw_bytes = 0
    total_new_templates = 0
    total_reused_span_count = 0
    total_span_count = 0
    total_evicted_templates = 0
    total_reintroduced_templates = 0
    template_last_window_seen: dict[int, int] = {}
    seen_signatures: set[tuple[object, ...]] = set()

    for window_index, start_index in enumerate(range(0, len(sessions), window_size), start=1):
        window_sessions = list(sessions[start_index : start_index + window_size])
        payload = codec.encode_sessions_incremental(window_sessions)
        payloads.append(payload)

        compressed_payload = zlib.compress(_compact_bytes(payload), level=9)
        raw_window_bytes = _compact_bytes([session.to_dict() for session in window_sessions])
        raw_window_zlib_bytes = len(zlib.compress(raw_window_bytes, level=9))

        new_template_ids = {int(template[0]) for template in payload["t"]}
        new_template_signatures = {_template_signature_from_compact(template) for template in payload["t"]}
        reintroduced_template_count = sum(1 for signature in new_template_signatures if signature in seen_signatures)
        total_reintroduced_templates += reintroduced_template_count
        seen_signatures.update(new_template_signatures)
        span_count = sum(len(session.spans) for session in window_sessions)
        reused_span_count = sum(
            1
            for encoded_session in payload["s"]
            for encoded_span in encoded_session["s"]
            if int(encoded_span[0]) not in new_template_ids
        )
        for encoded_session in payload["s"]:
            for encoded_span in encoded_session["s"]:
                template_last_window_seen[int(encoded_span[0])] = window_index

        evicted_template_count = 0
        if max_active_templates is not None and len(codec.templates) > max_active_templates:
            keep_template_ids = _select_retained_template_ids(
                codec=codec,
                template_last_window_seen=template_last_window_seen,
                max_active_templates=max_active_templates,
            )
            evicted_template_count = codec.prune_active_templates(keep_template_ids)
            total_evicted_templates += evicted_template_count
            template_last_window_seen = {
                template_id: last_seen
                for template_id, last_seen in template_last_window_seen.items()
                if template_id in keep_template_ids
            }

        manifest = {
            "window_index": window_index,
            "session_start_index": start_index,
            "session_end_index": start_index + len(window_sessions) - 1,
            "session_count": len(window_sessions),
            "span_count": span_count,
            "template_base_id": int(payload.get("template_base_id", 0)),
            "new_template_count": len(new_template_ids),
            "reintroduced_template_count": reintroduced_template_count,
            "template_count_after_window": len(codec.templates),
            "reused_span_count": reused_span_count,
            "reused_span_rate": round(reused_span_count / max(span_count, 1), 4),
            "raw_json_bytes": len(raw_window_bytes),
            "windowed_raw_zlib_bytes": raw_window_zlib_bytes,
            "stream_pack_bytes": len(compressed_payload),
            "stream_gain_vs_windowed_raw_zlib": raw_window_zlib_bytes - len(compressed_payload),
            "evicted_template_count": evicted_template_count,
        }
        window_packs.append(
            StreamingWindowPack(
                window_index=window_index,
                session_start_index=start_index,
                session_end_index=start_index + len(window_sessions) - 1,
                manifest=manifest,
                compressed_payload=compressed_payload,
            )
        )
        window_rows.append(manifest)

        total_stream_pack_bytes += len(compressed_payload)
        total_windowed_raw_zlib_bytes += raw_window_zlib_bytes
        total_windowed_raw_bytes += len(raw_window_bytes)
        total_new_templates += len(new_template_ids)
        total_reused_span_count += reused_span_count
        total_span_count += span_count

    decoded_sessions = decode_streaming_trace_packs([pack.compressed_payload for pack in window_packs])
    expected_sessions = [canonicalize_session(session) for session in sessions]
    if decoded_sessions != expected_sessions:
        raise ValueError("streaming trace packs failed canonical round trip")

    summary = {
        "window_size_sessions": window_size,
        "window_count": len(window_packs),
        "stream_total_raw_bytes": total_windowed_raw_bytes,
        "stream_windowed_raw_zlib_bytes": total_windowed_raw_zlib_bytes,
        "stream_pack_total_bytes": total_stream_pack_bytes,
        "stream_pack_ratio_vs_raw": round(total_stream_pack_bytes / max(total_windowed_raw_bytes, 1), 4),
        "stream_pack_gain_vs_windowed_raw_zlib": total_windowed_raw_zlib_bytes - total_stream_pack_bytes,
        "cross_window_template_reuse_rate": round(total_reused_span_count / max(total_span_count, 1), 4),
        "stream_new_template_total": total_new_templates,
        "stream_reintroduced_template_total": total_reintroduced_templates,
        "stream_evicted_template_total": total_evicted_templates,
        "final_active_template_count": len(codec.templates),
        "max_active_templates": max_active_templates if max_active_templates is not None else "unbounded",
        "stream_lossless_canonical_roundtrip": True,
    }
    return {
        "summary": summary,
        "window_rows": window_rows,
        "window_packs": window_packs,
    }


def write_streaming_trace_packs(
    stream_dir: Path,
    window_packs: Sequence[StreamingWindowPack],
) -> list[dict[str, object]]:
    stream_dir.mkdir(parents=True, exist_ok=True)
    artifact_rows: list[dict[str, object]] = []
    for pack in window_packs:
        manifest_path = stream_dir / f"window_{pack.window_index:03d}_manifest.json"
        payload_path = stream_dir / f"window_{pack.window_index:03d}_payload.zlib"
        manifest_path.write_text(json.dumps(pack.manifest, indent=2, sort_keys=True), encoding="utf-8")
        payload_path.write_bytes(pack.compressed_payload)
        artifact_rows.append(
            {
                "window_index": pack.window_index,
                "manifest_path": manifest_path.as_posix(),
                "payload_path": payload_path.as_posix(),
            }
        )
    return artifact_rows


def decode_streaming_trace_packs(compressed_payloads: Sequence[bytes]) -> list[dict[str, object]]:
    templates_by_id: dict[int, TraceTemplate] = {}
    decoded_sessions: list[dict[str, object]] = []

    for compressed_payload in compressed_payloads:
        payload = json.loads(zlib.decompress(compressed_payload).decode("utf-8"))
        for raw_template in payload.get("t", []):
            template = _trace_template_from_compact(raw_template)
            templates_by_id[template.template_id] = template
        decoded_sessions.extend(_decode_sessions_with_templates(payload, templates_by_id))
    return decoded_sessions


def load_streaming_trace_packs(stream_dir: Path) -> list[bytes]:
    payload_paths = sorted(Path(stream_dir).glob("window_*_payload.zlib"))
    return [payload_path.read_bytes() for payload_path in payload_paths]


def _decode_sessions_with_templates(
    payload: Mapping[str, object],
    templates_by_id: Mapping[int, TraceTemplate],
) -> list[dict[str, object]]:
    decoded_sessions: list[dict[str, object]] = []
    for encoded_session in payload.get("s", []):
        spans: list[dict[str, object]] = []
        for encoded_span in encoded_session.get("s", []):
            template = templates_by_id[int(encoded_span[0])]
            attributes = {
                key: _restore_json_value(value)
                for key, value in zip(template.attribute_keys, encoded_span[2], strict=False)
            }
            events = []
            for event_spec, raw_event_values in zip(template.event_specs, encoded_span[3], strict=False):
                event_name, attribute_keys = event_spec
                event_attributes = {
                    key: _restore_json_value(value)
                    for key, value in zip(attribute_keys, raw_event_values, strict=False)
                }
                events.append({"name": event_name, "attributes": event_attributes})
            spans.append(
                {
                    "parent_index": encoded_span[1],
                    "name": template.name,
                    "kind": template.kind,
                    "status": template.status,
                    "attributes": attributes,
                    "events": events,
                }
            )
        decoded_sessions.append({"workflow": encoded_session.get("w"), "spans": spans})
    return decoded_sessions


def _trace_template_from_compact(raw_template: Sequence[object]) -> TraceTemplate:
    return TraceTemplate(
        template_id=int(raw_template[0]),
        name=str(raw_template[1]),
        kind=str(raw_template[2]),
        status=str(raw_template[3]),
        attribute_keys=tuple(str(key) for key in raw_template[4]),
        event_specs=tuple(
            (str(spec[0]), tuple(str(key) for key in spec[1]))
            for spec in raw_template[5]
        ),
    )


def _template_signature_from_compact(raw_template: Sequence[object]) -> tuple[object, ...]:
    return (
        str(raw_template[1]),
        str(raw_template[2]),
        str(raw_template[3]),
        tuple(str(key) for key in raw_template[4]),
        tuple((str(spec[0]), tuple(str(key) for key in spec[1])) for spec in raw_template[5]),
    )


def _select_retained_template_ids(
    *,
    codec: SemanticTraceCodec,
    template_last_window_seen: Mapping[int, int],
    max_active_templates: int,
) -> set[int]:
    ranked_templates = sorted(
        codec.template_signatures_by_id,
        key=lambda template_id: (
            template_last_window_seen.get(template_id, -1),
            codec.template_use_counts.get(template_id, 0),
            -template_id,
        ),
        reverse=True,
    )
    return set(ranked_templates[:max_active_templates])


def _compact_bytes(payload: object) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _restore_json_value(value: object) -> object:
    if isinstance(value, list):
        return [_restore_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _restore_json_value(inner_value) for key, inner_value in value.items()}
    return value
