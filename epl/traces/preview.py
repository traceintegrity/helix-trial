from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from hashlib import sha1
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from epl.traces.schema import TraceSession


DEFAULT_POLICY_PATH = Path(__file__).with_name("trace_data_policy.json")


def build_safe_trace_preview(
    sessions: Sequence[TraceSession],
    *,
    session_limit: int | None = None,
    span_limit: int | None = None,
    policy_path: Path | None = None,
) -> dict[str, object]:
    policy = load_trace_data_policy(policy_path)
    preview_policy = policy["preview"]
    session_limit = int(preview_policy["session_limit"]) if session_limit is None else session_limit
    span_limit = int(preview_policy["span_limit"]) if span_limit is None else span_limit
    preview_sessions: list[dict[str, object]] = []
    redacted_fields = 0
    kept_fields = 0

    for session in sessions[:session_limit]:
        preview_spans: list[dict[str, object]] = []
        for span in session.spans[:span_limit]:
            redacted_attributes: dict[str, object] = {}
            for key, value in span.attributes.items():
                redacted_value, was_redacted = redact_preview_value(key, value, policy=policy)
                redacted_attributes[key] = redacted_value
                redacted_fields += int(was_redacted)
                kept_fields += int(not was_redacted)

            redacted_events = []
            for event in span.events:
                event_attributes: dict[str, object] = {}
                for key, value in event.attributes.items():
                    redacted_value, was_redacted = redact_preview_value(key, value, policy=policy)
                    event_attributes[key] = redacted_value
                    redacted_fields += int(was_redacted)
                    kept_fields += int(not was_redacted)
                redacted_events.append({"name": event.name, "attributes": event_attributes})

            preview_spans.append(
                {
                    "name": span.name,
                    "kind": span.kind,
                    "status": span.status,
                    "attributes": redacted_attributes,
                    "events": redacted_events,
                }
            )

        preview_sessions.append(
            {
                "session_id": session.session_id,
                "workflow": session.workflow,
                "spans": preview_spans,
            }
        )

    return {
        "policy_name": str(policy["policy_name"]),
        "bundle_payload_mode": str(policy["bundle_payload_mode"]),
        "session_count": len(sessions),
        "preview_session_count": len(preview_sessions),
        "preview_span_count": sum(len(session["spans"]) for session in preview_sessions),
        "redacted_field_count": redacted_fields,
        "kept_field_count": kept_fields,
        "sessions": preview_sessions,
    }


def load_trace_data_policy(policy_path: Path | None = None) -> dict[str, object]:
    path = Path(policy_path) if policy_path is not None else DEFAULT_POLICY_PATH
    payload = json.loads(path.read_text(encoding="utf-8"))
    preview_policy = dict(payload.get("preview") or {})
    preview_policy.setdefault("session_limit", 2)
    preview_policy.setdefault("span_limit", 8)
    preview_policy.setdefault("max_string_length", 120)
    preview_policy.setdefault(
        "redact_key_fragments",
        [
            "input",
            "output",
            "prompt",
            "completion",
            "response",
            "query",
            "statement",
            "content",
            "message",
            "argument",
            "url",
            "uri",
            "email",
        ],
    )
    return {
        "policy_name": str(payload.get("policy_name", "default_safe_preview_v1")),
        "bundle_payload_mode": str(payload.get("bundle_payload_mode", "full_fidelity")),
        "preview": preview_policy,
    }


def redact_preview_value(key: str, value: Any, *, policy: Mapping[str, object] | None = None) -> tuple[object, bool]:
    preview_policy = (policy or load_trace_data_policy())["preview"]
    if isinstance(value, str) and _is_sensitive_key(key, preview_policy["redact_key_fragments"]):
        return _redact_string(key, value), True
    if isinstance(value, str) and len(value) > int(preview_policy["max_string_length"]):
        return _summarize_text(value), True
    if isinstance(value, list):
        redacted_items = []
        redacted = False
        for item in value:
            redacted_item, item_redacted = redact_preview_value(key, item, policy=policy)
            redacted_items.append(redacted_item)
            redacted = redacted or item_redacted
        return redacted_items, redacted
    if isinstance(value, dict):
        redacted_map: dict[str, object] = {}
        redacted = False
        for inner_key, inner_value in value.items():
            redacted_value, item_redacted = redact_preview_value(f"{key}.{inner_key}", inner_value, policy=policy)
            redacted_map[str(inner_key)] = redacted_value
            redacted = redacted or item_redacted
        return redacted_map, redacted
    return value, False


def _is_sensitive_key(key: str, fragments: Sequence[str]) -> bool:
    normalized = key.lower()
    return any(fragment in normalized for fragment in fragments)


def _redact_string(key: str, value: str) -> str:
    token_count = len(value.split())
    digest = sha1(value.encode("utf-8")).hexdigest()[:10]
    if "url" in key.lower() or "uri" in key.lower():
        parsed = urlparse(value)
        host = parsed.netloc or "unknown-host"
        return f"<redacted:url host={host} tokens={token_count} sha={digest}>"
    return f"<redacted:text tokens={token_count} chars={len(value)} sha={digest}>"


def _summarize_text(value: str) -> str:
    token_count = len(value.split())
    digest = sha1(value.encode("utf-8")).hexdigest()[:10]
    return f"<summarized:text tokens={token_count} chars={len(value)} sha={digest}>"
