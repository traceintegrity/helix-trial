from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class TraceEvent:
    name: str
    attributes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "attributes": dict(self.attributes),
        }


@dataclass(frozen=True, slots=True)
class TraceSpan:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    kind: str
    status: str
    attributes: dict[str, Any] = field(default_factory=dict)
    events: tuple[TraceEvent, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "name": self.name,
            "kind": self.kind,
            "status": self.status,
            "attributes": dict(self.attributes),
            "events": [event.to_dict() for event in self.events],
        }


@dataclass(frozen=True, slots=True)
class TraceSession:
    session_id: str
    workflow: str
    spans: tuple[TraceSpan, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "workflow": self.workflow,
            "spans": [span.to_dict() for span in self.spans],
        }