import json
import gzip
import zlib
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from epl.traces.schema import TraceSession, TraceSpan


VOLATILE_ATTRIBUTE_KEYS = {
    "session.id",
    "trace.id",
    "span.id",
    "parent.span.id",
}


@dataclass(frozen=True, slots=True)
class TraceTemplate:
    template_id: int
    name: str
    kind: str
    status: str
    attribute_keys: tuple[str, ...]
    event_specs: tuple[tuple[str, tuple[str, ...]], ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "template_id": self.template_id,
            "name": self.name,
            "kind": self.kind,
            "status": self.status,
            "attribute_keys": list(self.attribute_keys),
            "event_specs": [
                {"name": name, "attribute_keys": list(attribute_keys)}
                for name, attribute_keys in self.event_specs
            ],
        }

    def to_compact(self) -> list[object]:
        return [
            self.template_id,
            self.name,
            self.kind,
            self.status,
            list(self.attribute_keys),
            [[name, list(attribute_keys)] for name, attribute_keys in self.event_specs],
        ]


@dataclass(slots=True)
class SemanticTraceCodec:
    templates: dict[tuple[object, ...], TraceTemplate]
    template_signatures_by_id: dict[int, tuple[object, ...]]
    template_use_counts: Counter[int]
    next_template_id: int

    def __init__(self) -> None:
        self.templates = {}
        self.template_signatures_by_id = {}
        self.template_use_counts = Counter()
        self.next_template_id = 1

    def encode_sessions(self, sessions: Sequence[TraceSession]) -> dict[str, object]:
        canonical_sessions = [canonicalize_session(session) for session in sessions]
        encoded_sessions = [self._encode_canonical_session(session) for session in canonical_sessions]
        templates = [template.to_compact() for _, template in sorted(self.templates.items(), key=lambda item: item[1].template_id)]
        return {
            "t": templates,
            "s": encoded_sessions,
        }

    def encode_sessions_incremental(self, sessions: Sequence[TraceSession]) -> dict[str, object]:
        canonical_sessions = [canonicalize_session(session) for session in sessions]
        new_templates: list[list[object]] = []
        encoded_sessions = [self._encode_canonical_session(session, new_templates) for session in canonical_sessions]
        return {
            "template_base_id": len(self.templates) - len(new_templates),
            "t": new_templates,
            "s": encoded_sessions,
        }

    def decode_sessions(self, payload: Mapping[str, object]) -> list[dict[str, object]]:
        templates_by_id: dict[int, TraceTemplate] = {}
        for raw_template in payload.get("t", []):
            template_id = int(raw_template[0])
            templates_by_id[template_id] = TraceTemplate(
                template_id=template_id,
                name=str(raw_template[1]),
                kind=str(raw_template[2]),
                status=str(raw_template[3]),
                attribute_keys=tuple(str(key) for key in raw_template[4]),
                event_specs=tuple(
                    (
                        str(spec[0]),
                        tuple(str(key) for key in spec[1]),
                    )
                    for spec in raw_template[5]
                ),
            )

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
            decoded_sessions.append(
                {
                    "workflow": encoded_session.get("w"),
                    "spans": spans,
                }
            )
        return decoded_sessions

    def _encode_canonical_session(
        self,
        session: Mapping[str, object],
        new_templates: list[list[object]] | None = None,
    ) -> dict[str, object]:
        encoded_spans = [self._encode_canonical_span(span, new_templates) for span in session["spans"]]
        return {
            "w": session["workflow"],
            "s": encoded_spans,
        }

    def _encode_canonical_span(
        self,
        span: Mapping[str, object],
        new_templates: list[list[object]] | None = None,
    ) -> dict[str, object]:
        signature = _template_signature(span)
        template = self.templates.get(signature)
        if template is None:
            template = TraceTemplate(
                template_id=self.next_template_id,
                name=str(span["name"]),
                kind=str(span["kind"]),
                status=str(span["status"]),
                attribute_keys=tuple(str(key) for key in span["attributes"].keys()),
                event_specs=tuple(
                    (
                        str(event["name"]),
                        tuple(str(key) for key in event["attributes"].keys()),
                    )
                    for event in span["events"]
                ),
            )
            self.templates[signature] = template
            self.template_signatures_by_id[template.template_id] = signature
            self.next_template_id += 1
            if new_templates is not None:
                new_templates.append(template.to_compact())
        self.template_use_counts[template.template_id] += 1

        encoded_events = []
        for event in span["events"]:
            encoded_events.append([_json_safe_value(value) for value in event["attributes"].values()])

        return [
            template.template_id,
            span["parent_index"],
            [_json_safe_value(value) for value in span["attributes"].values()],
            encoded_events,
        ]

    def prune_active_templates(self, keep_template_ids: set[int]) -> int:
        removed_signatures = [
            signature
            for signature, template in self.templates.items()
            if template.template_id not in keep_template_ids
        ]
        for signature in removed_signatures:
            template = self.templates.pop(signature)
            self.template_signatures_by_id.pop(template.template_id, None)
            self.template_use_counts.pop(template.template_id, None)
        return len(removed_signatures)


def benchmark_trace_codec(sessions: Sequence[TraceSession]) -> dict[str, object]:
    if not sessions:
        raise ValueError("sessions must not be empty")

    raw_payload = [session.to_dict() for session in sessions]
    canonical_payload = [canonicalize_session(session) for session in sessions]
    codec = SemanticTraceCodec()
    semantic_payload = codec.encode_sessions(sessions)
    decoded_payload = codec.decode_sessions(semantic_payload)
    if decoded_payload != canonical_payload:
        raise ValueError("semantic payload failed canonical round trip")
    reproducibility_codec = SemanticTraceCodec()
    reproducibility_payload = reproducibility_codec.encode_sessions(sessions)
    reproducibility_verified = semantic_payload == reproducibility_payload

    raw_bytes = _compact_bytes(raw_payload)
    canonical_bytes = _compact_bytes(canonical_payload)
    semantic_bytes = _compact_bytes(semantic_payload)
    zlib_raw_bytes = len(zlib.compress(raw_bytes, level=9))
    zlib_canonical_bytes = len(zlib.compress(canonical_bytes, level=9))
    zlib_semantic_bytes = len(zlib.compress(semantic_bytes, level=9))
    gzip_raw_bytes = len(gzip.compress(raw_bytes, compresslevel=9, mtime=0))
    gzip_canonical_bytes = len(gzip.compress(canonical_bytes, compresslevel=9, mtime=0))
    gzip_semantic_bytes = len(gzip.compress(semantic_bytes, compresslevel=9, mtime=0))

    workflow_counts = Counter(session.workflow for session in sessions)
    template_rows = []
    for template in sorted(codec.templates.values(), key=lambda item: (-codec.template_use_counts[item.template_id], item.template_id)):
        template_rows.append(
            {
                "template_id": template.template_id,
                "name": template.name,
                "kind": template.kind,
                "status": template.status,
                "attribute_keys": list(template.attribute_keys),
                "event_specs": [
                    {"name": name, "attribute_keys": list(attribute_keys)}
                    for name, attribute_keys in template.event_specs
                ],
                "uses": codec.template_use_counts[template.template_id],
            }
        )

    span_count = sum(len(session.spans) for session in sessions)
    summary = {
        "session_count": len(sessions),
        "workflow_count": len(workflow_counts),
        "span_count": span_count,
        "template_count": len(template_rows),
        "avg_spans_per_session": round(span_count / len(sessions), 2),
        "template_reuse_rate": round(1.0 - (len(template_rows) / max(span_count, 1)), 4),
        "raw_json_bytes": len(raw_bytes),
        "canonical_json_bytes": len(canonical_bytes),
        "semantic_json_bytes": len(semantic_bytes),
        "zlib_raw_bytes": zlib_raw_bytes,
        "zlib_canonical_bytes": zlib_canonical_bytes,
        "zlib_semantic_bytes": zlib_semantic_bytes,
        "gzip_raw_bytes": gzip_raw_bytes,
        "gzip_canonical_bytes": gzip_canonical_bytes,
        "gzip_semantic_bytes": gzip_semantic_bytes,
        "canonical_ratio_vs_raw": round(len(canonical_bytes) / max(len(raw_bytes), 1), 4),
        "semantic_ratio_vs_raw": round(len(semantic_bytes) / max(len(raw_bytes), 1), 4),
        "zlib_raw_ratio_vs_raw": round(zlib_raw_bytes / max(len(raw_bytes), 1), 4),
        "zlib_canonical_ratio_vs_raw": round(zlib_canonical_bytes / max(len(raw_bytes), 1), 4),
        "zlib_semantic_ratio_vs_raw": round(zlib_semantic_bytes / max(len(raw_bytes), 1), 4),
        "gzip_raw_ratio_vs_raw": round(gzip_raw_bytes / max(len(raw_bytes), 1), 4),
        "gzip_canonical_ratio_vs_raw": round(gzip_canonical_bytes / max(len(raw_bytes), 1), 4),
        "gzip_semantic_ratio_vs_raw": round(gzip_semantic_bytes / max(len(raw_bytes), 1), 4),
        "semantic_plus_zlib_gain_vs_raw_zlib": zlib_raw_bytes - zlib_semantic_bytes,
        "semantic_plus_gzip_gain_vs_raw_gzip": gzip_raw_bytes - gzip_semantic_bytes,
        "canonical_roundtrip_verified": int(True),
        "reproducibility_verified": int(reproducibility_verified),
        "canonical_sha256": sha256(canonical_bytes).hexdigest(),
        "semantic_sha256": sha256(semantic_bytes).hexdigest(),
    }
    return {
        "summary": summary,
        "raw_payload": raw_payload,
        "canonical_payload": canonical_payload,
        "semantic_payload": semantic_payload,
        "template_rows": template_rows,
        "workflow_rows": [
            {"workflow": workflow, "session_count": workflow_counts[workflow]}
            for workflow in sorted(workflow_counts)
        ],
    }


def canonicalize_session(session: TraceSession) -> dict[str, object]:
    span_index_by_id = {span.span_id: index for index, span in enumerate(session.spans)}
    canonical_spans = [canonicalize_span(span, span_index_by_id) for span in session.spans]
    return {
        "workflow": session.workflow,
        "spans": canonical_spans,
    }


def canonicalize_span(span: TraceSpan, span_index_by_id: Mapping[str, int]) -> dict[str, object]:
    attributes = {
        key: _json_safe_value(value)
        for key, value in sorted(span.attributes.items())
        if key not in VOLATILE_ATTRIBUTE_KEYS
    }
    events = []
    for event in span.events:
        events.append(
            {
                "name": event.name,
                "attributes": {
                    key: _json_safe_value(value)
                    for key, value in sorted(event.attributes.items())
                },
            }
        )
    parent_index = None if span.parent_span_id is None else span_index_by_id.get(span.parent_span_id)
    return {
        "parent_index": parent_index,
        "name": span.name,
        "kind": span.kind,
        "status": span.status,
        "attributes": attributes,
        "events": events,
    }


def _template_signature(span: Mapping[str, object]) -> tuple[object, ...]:
    return (
        span["name"],
        span["kind"],
        span["status"],
        tuple(span["attributes"].keys()),
        tuple((event["name"], tuple(event["attributes"].keys())) for event in span["events"]),
    )


def _compact_bytes(payload: object) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _json_safe_value(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _json_safe_value(inner_value) for key, inner_value in sorted(value.items())}
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe_value(item) for item in value]
    return value


def _restore_json_value(value: object) -> object:
    if isinstance(value, list):
        return [_restore_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _restore_json_value(inner_value) for key, inner_value in value.items()}
    return value
