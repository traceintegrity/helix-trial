import json
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from epl.traces.schema import TraceEvent, TraceSession, TraceSpan


WORKFLOWS = ("rag_support", "sql_analytics", "browser_research")
OTLP_SPAN_KIND_MAP = {
    0: "UNSPECIFIED",
    1: "INTERNAL",
    2: "SERVER",
    3: "CLIENT",
    4: "PRODUCER",
    5: "CONSUMER",
}
OTLP_STATUS_CODE_MAP = {
    0: "UNSET",
    1: "OK",
    2: "ERROR",
}


def build_trace_fixture_sessions(session_count: int = 18) -> list[TraceSession]:
    if session_count <= 0:
        raise ValueError("session_count must be greater than zero")

    sessions: list[TraceSession] = []
    for index in range(session_count):
        workflow = WORKFLOWS[index % len(WORKFLOWS)]
        if workflow == "rag_support":
            sessions.append(_build_rag_support_session(index))
        elif workflow == "sql_analytics":
            sessions.append(_build_sql_analytics_session(index))
        else:
            sessions.append(_build_browser_research_session(index))
    return sessions


def detect_trace_input_format(path: Path) -> str:
    suffix = Path(path).suffix.lower()
    if suffix == ".jsonl":
        return "jsonl_span_rows"
    if suffix != ".json":
        return "unknown"

    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if isinstance(payload, dict) and "resourceSpans" in payload:
        return "otlp_json"
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict) and "resourceSpans" in payload["data"]:
        return "otlp_json"
    if isinstance(payload, dict) and isinstance(payload.get("trace"), dict) and isinstance(payload.get("observations"), list):
        return "langfuse_trace_export"
    if isinstance(payload, list):
        return "json_span_array"
    return "unknown_json"


def load_trace_sessions(path: Path, *, input_format: str | None = None) -> list[TraceSession]:
    input_format = input_format or detect_trace_input_format(path)
    if input_format == "jsonl_span_rows":
        return load_trace_sessions_jsonl(path)
    if input_format == "otlp_json":
        return load_trace_sessions_otlp_json(path)
    if input_format == "langfuse_trace_export":
        return load_trace_sessions_langfuse_export(path)
    if input_format == "json_span_array":
        return load_trace_sessions_json_span_array(path)
    raise ValueError(f"unsupported trace input format for {path}")


def load_trace_sessions_json_span_array(path: Path) -> list[TraceSession]:
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, list):
        raise ValueError("expected JSON span array payload")
    return _load_trace_sessions_from_span_payloads(payload)


def load_trace_sessions_jsonl(path: Path) -> list[TraceSession]:
    payloads: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            payloads.append(json.loads(stripped))
    return _load_trace_sessions_from_span_payloads(payloads)


def load_trace_sessions_otlp_json(path: Path) -> list[TraceSession]:
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        payload = payload["data"]
    if not isinstance(payload, dict) or not isinstance(payload.get("resourceSpans"), list):
        raise ValueError("expected OTLP JSON payload with resourceSpans")

    rows: list[dict[str, Any]] = []
    row_index = 0
    for resource_span in payload["resourceSpans"]:
        resource_attributes = _otlp_attributes_to_dict(resource_span.get("resource", {}).get("attributes", []))
        scope_spans = resource_span.get("scopeSpans") or resource_span.get("instrumentationLibrarySpans") or []
        for scope_span in scope_spans:
            scope = scope_span.get("scope") or scope_span.get("instrumentationLibrary") or {}
            scope_name = str(scope.get("name") or "")
            scope_version = str(scope.get("version") or "")
            for raw_span in scope_span.get("spans") or []:
                span_attributes = _otlp_attributes_to_dict(raw_span.get("attributes", []))
                merged_attributes = {
                    **resource_attributes,
                    **span_attributes,
                }
                if scope_name:
                    merged_attributes["otel.scope.name"] = scope_name
                if scope_version:
                    merged_attributes["otel.scope.version"] = scope_version

                rows.append(
                    {
                        "session_id": (
                            merged_attributes.get("session.id")
                            or merged_attributes.get("conversation.id")
                            or merged_attributes.get("thread.id")
                            or raw_span.get("traceId")
                            or f"unknown-session-{row_index}"
                        ),
                        "workflow": (
                            merged_attributes.get("workflow.name")
                            or merged_attributes.get("agent.workflow")
                            or merged_attributes.get("service.name")
                            or "unknown_workflow"
                        ),
                        "trace_id": raw_span.get("traceId") or merged_attributes.get("trace.id") or "trace-unknown",
                        "span_id": raw_span.get("spanId") or f"span-{row_index:06d}",
                        "parent_span_id": _optional_str(raw_span.get("parentSpanId")),
                        "name": raw_span.get("name") or "span.unknown",
                        "kind": (
                            merged_attributes.get("openinference.span.kind")
                            or OTLP_SPAN_KIND_MAP.get(_coerce_int(raw_span.get("kind")), "INTERNAL")
                        ),
                        "status": _map_otlp_status(raw_span.get("status")),
                        "attributes": merged_attributes,
                        "events": [
                            {
                                "name": event.get("name") or "event.unknown",
                                "attributes": _otlp_attributes_to_dict(event.get("attributes", [])),
                            }
                            for event in raw_span.get("events") or []
                        ],
                        "_sort_index": _coerce_int(raw_span.get("startTimeUnixNano"), default=row_index),
                    }
                )
                row_index += 1

    return _load_trace_sessions_from_span_payloads(rows)


def load_trace_sessions_langfuse_export(path: Path) -> list[TraceSession]:
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict) or not isinstance(payload.get("observations"), list):
        raise ValueError("expected Langfuse trace export payload with observations")

    trace = payload.get("trace") or {}
    trace_metadata = _coerce_mapping(_parse_jsonish(trace.get("metadata")))
    trace_attributes = _build_langfuse_trace_attributes(trace=trace, trace_metadata=trace_metadata)
    session_id = str(
        trace.get("sessionId")
        or trace.get("id")
        or Path(path).stem
    )
    workflow = str(
        trace.get("name")
        or trace_attributes.get("workflow.name")
        or trace_attributes.get("service.name")
        or "langfuse_trace"
    )
    trace_id = str(trace.get("id") or session_id)

    rows: list[dict[str, Any]] = []
    for row_index, observation in enumerate(payload["observations"]):
        if not isinstance(observation, dict):
            continue

        observation_metadata = _coerce_mapping(observation.get("metadata"))
        observation_attributes = {
            **trace_attributes,
            **_build_langfuse_observation_attributes(observation=observation, observation_metadata=observation_metadata),
        }
        rows.append(
            {
                "session_id": session_id,
                "workflow": workflow,
                "trace_id": str(observation.get("traceId") or trace_id),
                "span_id": str(observation.get("id") or f"{trace_id}-span-{row_index:06d}"),
                "parent_span_id": _optional_str(observation.get("parentObservationId")),
                "name": str(observation.get("name") or observation.get("type") or "span.unknown"),
                "kind": (
                    observation_attributes.get("openinference.span.kind")
                    or observation.get("type")
                    or trace_attributes.get("openinference.span.kind")
                    or "SPAN"
                ),
                "status": _map_langfuse_status(observation),
                "attributes": observation_attributes,
                "events": _normalize_event_payloads(observation.get("events")),
                "_sort_index": _coerce_sort_index(observation.get("startTime"), default=row_index),
            }
        )

    return _load_trace_sessions_from_span_payloads(rows)


def _load_trace_sessions_from_span_payloads(payloads: list[dict[str, Any]]) -> list[TraceSession]:
    sessions_by_id: dict[str, dict[str, Any]] = {}
    for row_index, raw_payload in enumerate(payloads):
        payload = _normalize_span_payload(raw_payload, row_index=row_index)
        attributes = payload.get("attributes") or {}
        session_id = str(
            payload.get("session_id")
            or attributes.get("session.id")
            or attributes.get("conversation.id")
            or attributes.get("thread.id")
            or payload.get("trace_id")
            or attributes.get("trace.id")
            or f"unknown-session-{row_index}"
        )
        workflow = str(
            payload.get("workflow")
            or attributes.get("workflow.name")
            or attributes.get("agent.workflow")
            or attributes.get("service.name")
            or attributes.get("openinference.span.kind")
            or "unknown_workflow"
        )
        bucket = sessions_by_id.setdefault(
            session_id,
            {
                "workflow": workflow,
                "spans": [],
            },
        )
        if bucket["workflow"] == "unknown_workflow" and workflow != "unknown_workflow":
            bucket["workflow"] = workflow
        bucket["spans"].append((_coerce_int(payload.get("_sort_index"), default=row_index), _coerce_span(payload)))

    sessions: list[TraceSession] = []
    for session_id in sorted(sessions_by_id):
        bucket = sessions_by_id[session_id]
        spans = tuple(
            span
            for _, span in sorted(
                bucket["spans"],
                key=lambda item: (item[0], item[1].span_id),
            )
        )
        sessions.append(TraceSession(session_id=session_id, workflow=str(bucket["workflow"]), spans=spans))
    return sessions


def _coerce_span(payload: dict[str, Any]) -> TraceSpan:
    attributes = payload.get("attributes") or {}
    raw_events = payload.get("events") or []
    return TraceSpan(
        trace_id=str(payload.get("trace_id") or attributes.get("trace.id") or "trace-unknown"),
        span_id=str(payload.get("span_id") or "span-unknown"),
        parent_span_id=_optional_str(payload.get("parent_span_id")),
        name=str(payload.get("name") or "span.unknown"),
        kind=str(payload.get("kind") or attributes.get("openinference.span.kind") or "CHAIN"),
        status=str(payload.get("status") or "OK"),
        attributes={str(key): value for key, value in attributes.items()},
        events=tuple(
            TraceEvent(
                name=str(event.get("name") or "event.unknown"),
                attributes={str(key): value for key, value in (event.get("attributes") or {}).items()},
            )
            for event in raw_events
        ),
    )


def _normalize_span_payload(payload: dict[str, Any], *, row_index: int) -> dict[str, Any]:
    attributes = _normalize_attribute_mapping(payload.get("attributes"))
    resource_attributes = _normalize_attribute_mapping(_coerce_mapping(payload.get("resource")).get("attributes"))
    merged_attributes = {**resource_attributes, **attributes}

    session_id = _extract_session_id(payload=payload, attributes=merged_attributes, row_index=row_index)
    workflow = _extract_workflow(payload=payload, attributes=merged_attributes)
    trace_id = str(
        payload.get("trace_id")
        or payload.get("traceId")
        or merged_attributes.get("trace.id")
        or session_id
        or f"trace-{row_index:06d}"
    )
    span_id = str(
        payload.get("span_id")
        or payload.get("spanId")
        or merged_attributes.get("span.id")
        or f"span-{row_index:06d}"
    )
    kind = _extract_kind(payload=payload, attributes=merged_attributes)
    status = _extract_status(payload)

    return {
        "session_id": session_id,
        "workflow": workflow,
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": _optional_str(payload.get("parent_span_id") or payload.get("parentSpanId")),
        "name": str(payload.get("name") or payload.get("operationName") or "span.unknown"),
        "kind": kind,
        "status": status,
        "attributes": merged_attributes,
        "events": _normalize_event_payloads(payload.get("events")),
        "_sort_index": _coerce_sort_index(
            payload.get("_sort_index")
            if "_sort_index" in payload
            else payload.get("startTimeUnixNano") or payload.get("startTime"),
            default=row_index,
        ),
    }


def _otlp_attributes_to_dict(attributes: Any) -> dict[str, Any]:
    if not isinstance(attributes, list):
        return {}
    result: dict[str, Any] = {}
    for item in attributes:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "")
        if not key:
            continue
        result[key] = _otlp_value_to_python(item.get("value"))
    return result


def _otlp_value_to_python(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    if "stringValue" in value:
        return str(value["stringValue"])
    if "boolValue" in value:
        return bool(value["boolValue"])
    if "intValue" in value:
        return _coerce_int(value["intValue"])
    if "doubleValue" in value:
        return float(value["doubleValue"])
    if "arrayValue" in value:
        return [_otlp_value_to_python(item) for item in value["arrayValue"].get("values", [])]
    if "kvlistValue" in value:
        return {
            str(item.get("key") or ""): _otlp_value_to_python(item.get("value"))
            for item in value["kvlistValue"].get("values", [])
            if isinstance(item, dict)
        }
    if "bytesValue" in value:
        return str(value["bytesValue"])
    return {str(key): _otlp_value_to_python(inner_value) for key, inner_value in value.items()}


def _map_otlp_status(status: Any) -> str:
    if isinstance(status, dict):
        code = _coerce_int(status.get("code"))
        return OTLP_STATUS_CODE_MAP.get(code, str(status.get("message") or "UNSET") or "UNSET")
    return str(status or "UNSET")


def _map_langfuse_status(observation: Mapping[str, Any]) -> str:
    level = str(observation.get("level") or "").upper()
    if level in {"ERROR", "FAILURE"}:
        return "ERROR"
    if observation.get("statusMessage"):
        return "ERROR"
    return "OK"


def _extract_session_id(*, payload: dict[str, Any], attributes: dict[str, Any], row_index: int) -> str:
    direct_candidates = (
        payload.get("session_id"),
        payload.get("sessionId"),
        attributes.get("session.id"),
        attributes.get("conversation.id"),
        attributes.get("thread.id"),
        attributes.get("threadId"),
        attributes.get("sessionId"),
        attributes.get("http.request_id"),
        payload.get("trace_id"),
        payload.get("traceId"),
        attributes.get("trace.id"),
    )
    for candidate in direct_candidates:
        if candidate not in {None, "", "null"}:
            return str(candidate)

    for value in attributes.values():
        nested_candidate = _find_nested_identifier(_parse_jsonish(value), ("threadId", "sessionId", "request_id", "requestId"))
        if nested_candidate is not None:
            return nested_candidate

    return f"unknown-session-{row_index}"


def _extract_workflow(*, payload: dict[str, Any], attributes: dict[str, Any]) -> str:
    direct_candidates = (
        payload.get("workflow"),
        attributes.get("workflow.name"),
        attributes.get("agent.workflow"),
        attributes.get("componentName"),
        attributes.get("resourceId"),
        attributes.get("agent.name"),
        attributes.get("service.name"),
        attributes.get("openinference.span.kind"),
    )
    for candidate in direct_candidates:
        if candidate not in {None, "", "null"}:
            return str(candidate)

    for value in attributes.values():
        nested_candidate = _find_nested_identifier(_parse_jsonish(value), ("resourceId", "componentName", "agentName"))
        if nested_candidate is not None:
            return nested_candidate

    return "unknown_workflow"


def _extract_kind(*, payload: dict[str, Any], attributes: dict[str, Any]) -> str:
    if attributes.get("openinference.span.kind") not in {None, "", "null"}:
        return str(attributes["openinference.span.kind"])
    raw_kind = payload.get("kind")
    if isinstance(raw_kind, int):
        return OTLP_SPAN_KIND_MAP.get(raw_kind, "INTERNAL")
    if isinstance(raw_kind, str) and raw_kind:
        return raw_kind
    return "CHAIN"


def _extract_status(payload: dict[str, Any]) -> str:
    if "status" in payload:
        return _map_otlp_status(payload.get("status"))
    if "level" in payload:
        return _map_langfuse_status(payload)
    return "UNSET"


def _normalize_event_payloads(events: Any) -> list[dict[str, Any]]:
    if not isinstance(events, list):
        return []
    normalized: list[dict[str, Any]] = []
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            continue
        normalized.append(
            {
                "name": str(event.get("name") or event.get("event") or f"event-{index}"),
                "attributes": _normalize_attribute_mapping(event.get("attributes")),
            }
        )
    return normalized


def _build_langfuse_trace_attributes(*, trace: dict[str, Any], trace_metadata: dict[str, Any]) -> dict[str, Any]:
    attributes: dict[str, Any] = {
        "langfuse.trace.name": str(trace.get("name") or ""),
        "langfuse.trace.environment": str(trace.get("environment") or ""),
        "langfuse.trace.public": bool(trace.get("public", False)),
    }
    if trace.get("input") is not None:
        attributes["trace.input"] = _normalize_attribute_value(_parse_jsonish(trace.get("input")))
    if trace.get("output") is not None:
        attributes["trace.output"] = _normalize_attribute_value(_parse_jsonish(trace.get("output")))

    attributes.update(_normalize_attribute_mapping(trace_metadata.get("resourceAttributes")))
    attributes.update(_normalize_attribute_mapping(trace_metadata.get("attributes")))

    scope = _coerce_mapping(trace_metadata.get("scope"))
    if scope.get("name") not in {None, ""}:
        attributes["otel.scope.name"] = str(scope.get("name"))
    if scope.get("version") not in {None, ""}:
        attributes["otel.scope.version"] = str(scope.get("version"))
    attributes.update(_flatten_nested_mapping(scope.get("attributes"), prefix="otel.scope.attribute"))

    return {key: value for key, value in attributes.items() if value not in {None, ""}}


def _build_langfuse_observation_attributes(
    *,
    observation: dict[str, Any],
    observation_metadata: dict[str, Any],
) -> dict[str, Any]:
    attributes = {
        "langfuse.observation.type": str(observation.get("type") or ""),
        "langfuse.observation.level": str(observation.get("level") or ""),
    }
    for field_name in ("inputCost", "outputCost", "inputUsage", "outputUsage", "latency"):
        if observation.get(field_name) is not None:
            attributes[f"langfuse.{field_name}"] = observation.get(field_name)
    if observation.get("statusMessage"):
        attributes["langfuse.status_message"] = str(observation.get("statusMessage"))
    if observation.get("input") is not None:
        attributes["input.value"] = _normalize_attribute_value(_parse_jsonish(observation.get("input")))
    if observation.get("output") is not None:
        attributes["output.value"] = _normalize_attribute_value(_parse_jsonish(observation.get("output")))

    direct_attributes = _coerce_mapping(observation_metadata.get("attributes"))
    attributes.update(_normalize_attribute_mapping(direct_attributes))
    attributes.update(_flatten_nested_mapping(observation_metadata, prefix="langfuse.metadata", skip_keys={"attributes"}))

    return {key: value for key, value in attributes.items() if value not in {None, ""}}


def _normalize_attribute_mapping(attributes: Any) -> dict[str, Any]:
    if isinstance(attributes, list):
        return _otlp_attributes_to_dict(attributes)
    if not isinstance(attributes, dict):
        return {}
    result: dict[str, Any] = {}
    for key, value in attributes.items():
        result[str(key)] = _normalize_attribute_value(value)
    return result


def _normalize_attribute_value(value: Any) -> Any:
    value = _parse_jsonish(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return value


def _parse_jsonish(value: Any, *, depth: int = 0) -> Any:
    if depth >= 2 or not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return value
    if stripped[0] not in {'{', '[', '"'}:
        return value
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return value
    if isinstance(parsed, str):
        return _parse_jsonish(parsed, depth=depth + 1)
    return parsed


def _coerce_mapping(value: Any) -> dict[str, Any]:
    parsed = _parse_jsonish(value)
    if isinstance(parsed, dict):
        return parsed
    return {}


def _flatten_nested_mapping(value: Any, *, prefix: str, skip_keys: set[str] | None = None) -> dict[str, Any]:
    skip_keys = skip_keys or set()
    result: dict[str, Any] = {}
    if not isinstance(value, dict):
        return result
    for key, nested_value in value.items():
        if str(key) in skip_keys:
            continue
        nested_prefix = f"{prefix}.{key}"
        if isinstance(nested_value, dict):
            result.update(_flatten_nested_mapping(nested_value, prefix=nested_prefix))
            continue
        result[nested_prefix] = _normalize_attribute_value(nested_value)
    return result


def _find_nested_identifier(value: Any, keys: tuple[str, ...]) -> str | None:
    if isinstance(value, dict):
        for key in keys:
            candidate = value.get(key)
            if candidate not in {None, "", "null"}:
                return str(candidate)
        for nested_value in value.values():
            candidate = _find_nested_identifier(nested_value, keys)
            if candidate is not None:
                return candidate
    if isinstance(value, list):
        for nested_value in value:
            candidate = _find_nested_identifier(nested_value, keys)
            if candidate is not None:
                return candidate
    return None


def _coerce_sort_index(value: Any, default: int = 0) -> int:
    if isinstance(value, list) and len(value) >= 2:
        return _coerce_int(value[0], default=0) * 1_000_000_000 + _coerce_int(value[1], default=0)
    if isinstance(value, str):
        stripped = value.strip()
        if re.fullmatch(r"-?\d+", stripped):
            return _coerce_int(stripped, default=default)
        try:
            parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
        except ValueError:
            return default
        return int(parsed.astimezone(timezone.utc).timestamp() * 1_000_000_000)
    return _coerce_int(value, default=default)


def _coerce_int(value: Any, default: int = 0) -> int:
    if value in {None, ""}:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _optional_str(value: object) -> str | None:
    if value in {None, "", "null"}:
        return None
    return str(value)


def _build_rag_support_session(index: int) -> TraceSession:
    session_id = f"rag-session-{index:04d}"
    trace_id = f"trace-rag-{index:04d}"
    question = f"Refund policy for order group {index % 4} in region {index % 3}?"
    doc_ids = [f"policy-{index % 5}", f"faq-{(index + 1) % 6}"]
    final_answer = f"Customer should use refund flow tier {(index % 3) + 1} with policy {doc_ids[0]}."
    spans = (
        _make_span(
            trace_id,
            "s0",
            None,
            "workflow.execute",
            "CHAIN",
            {
                "service.name": "semantic-compression-lab",
                "session.id": session_id,
                "workflow.name": "rag_support",
                "input.value": question,
                "output.value": final_answer,
            },
            events=(TraceEvent("handoff.started", {"queue": "support"}),),
        ),
        _make_span(
            trace_id,
            "s1",
            "s0",
            "agent.plan",
            "AGENT",
            {
                "service.name": "semantic-compression-lab",
                "agent.name": "support-triage",
                "input.value": question,
                "output.value": "retrieve policy then answer",
            },
        ),
        _make_span(
            trace_id,
            "s2",
            "s1",
            "retriever.lookup",
            "RETRIEVER",
            {
                "service.name": "semantic-compression-lab",
                "retrieval.top_k": 2,
                "input.value": question,
                "output.value": doc_ids,
            },
            events=(TraceEvent("retrieval.completed", {"document.count": 2}),),
        ),
        _make_span(
            trace_id,
            "s3",
            "s1",
            "llm.reason",
            "LLM",
            {
                "service.name": "semantic-compression-lab",
                "llm.model_name": "open-weights-70b",
                "input.value": f"Question: {question} Docs: {doc_ids}",
                "output.value": "refund flow identified",
            },
            events=(TraceEvent("llm.usage", {"prompt_tokens": 220 + index, "completion_tokens": 38}),),
        ),
        _make_span(
            trace_id,
            "s4",
            "s1",
            "agent.respond",
            "AGENT",
            {
                "service.name": "semantic-compression-lab",
                "agent.name": "support-triage",
                "input.value": question,
                "output.value": final_answer,
            },
        ),
    )
    return TraceSession(session_id=session_id, workflow="rag_support", spans=spans)


def _build_sql_analytics_session(index: int) -> TraceSession:
    session_id = f"sql-session-{index:04d}"
    trace_id = f"trace-sql-{index:04d}"
    city = ("Toronto", "Vancouver", "Calgary")[index % 3]
    query = f"Revenue by month for {city} in cohort {(index % 4) + 1}"
    sql = f"SELECT month, revenue FROM sales WHERE city = '{city}' AND cohort = {(index % 4) + 1}"
    answer = f"{city} peaks in month {(index % 6) + 1} with stable revenue trend."
    spans = (
        _make_span(
            trace_id,
            "s0",
            None,
            "workflow.execute",
            "CHAIN",
            {
                "service.name": "semantic-compression-lab",
                "session.id": session_id,
                "workflow.name": "sql_analytics",
                "input.value": query,
                "output.value": answer,
            },
        ),
        _make_span(
            trace_id,
            "s1",
            "s0",
            "agent.plan",
            "AGENT",
            {
                "service.name": "semantic-compression-lab",
                "agent.name": "analytics-agent",
                "input.value": query,
                "output.value": "write sql and summarize trend",
            },
        ),
        _make_span(
            trace_id,
            "s2",
            "s1",
            "tool.sql.execute",
            "TOOL",
            {
                "service.name": "semantic-compression-lab",
                "tool.name": "warehouse.query",
                "input.value": sql,
                "output.value": f"12 rows for {city}",
            },
            events=(TraceEvent("tool.result", {"row.count": 12}),),
        ),
        _make_span(
            trace_id,
            "s3",
            "s1",
            "llm.summarize",
            "LLM",
            {
                "service.name": "semantic-compression-lab",
                "llm.model_name": "open-weights-70b",
                "input.value": f"Summarize result set for {city}",
                "output.value": answer,
            },
            events=(TraceEvent("llm.usage", {"prompt_tokens": 180 + index, "completion_tokens": 32}),),
        ),
    )
    return TraceSession(session_id=session_id, workflow="sql_analytics", spans=spans)


def _build_browser_research_session(index: int) -> TraceSession:
    session_id = f"browser-session-{index:04d}"
    trace_id = f"trace-browser-{index:04d}"
    company = ("Acme Robotics", "Northwind Bio", "Atlas Grid")[index % 3]
    task = f"Collect public facts about {company} and summarize launch risks"
    url = f"https://example.org/reports/{company.lower().replace(' ', '-')}/{index % 5}"
    answer = f"{company} shows moderate launch risk with dependence on supplier tier {(index % 3) + 1}."
    spans = (
        _make_span(
            trace_id,
            "s0",
            None,
            "workflow.execute",
            "CHAIN",
            {
                "service.name": "semantic-compression-lab",
                "session.id": session_id,
                "workflow.name": "browser_research",
                "input.value": task,
                "output.value": answer,
            },
        ),
        _make_span(
            trace_id,
            "s1",
            "s0",
            "browser.fetch",
            "TOOL",
            {
                "service.name": "semantic-compression-lab",
                "tool.name": "browser.fetch",
                "input.value": url,
                "output.value": f"html snapshot {index % 7}",
            },
            events=(TraceEvent("tool.result", {"byte.count": 18000 + index * 17}),),
        ),
        _make_span(
            trace_id,
            "s2",
            "s0",
            "agent.extract",
            "AGENT",
            {
                "service.name": "semantic-compression-lab",
                "agent.name": "research-agent",
                "input.value": task,
                "output.value": "evidence extracted",
            },
        ),
        _make_span(
            trace_id,
            "s3",
            "s2",
            "llm.summarize",
            "LLM",
            {
                "service.name": "semantic-compression-lab",
                "llm.model_name": "open-weights-70b",
                "input.value": f"Summarize browser evidence for {company}",
                "output.value": answer,
            },
            events=(TraceEvent("llm.usage", {"prompt_tokens": 260 + index, "completion_tokens": 41}),),
        ),
    )
    return TraceSession(session_id=session_id, workflow="browser_research", spans=spans)


def _make_span(
    trace_id: str,
    span_id: str,
    parent_span_id: str | None,
    name: str,
    kind: str,
    attributes: dict[str, Any],
    *,
    status: str = "OK",
    events: tuple[TraceEvent, ...] = (),
) -> TraceSpan:
    enriched_attributes = {
        **attributes,
        "openinference.span.kind": kind,
    }
    return TraceSpan(
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent_span_id,
        name=name,
        kind=kind,
        status=status,
        attributes=enriched_attributes,
        events=events,
    )
