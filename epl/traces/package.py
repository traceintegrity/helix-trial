from __future__ import annotations

import json
import zlib
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256

from epl.traces.codec import SemanticTraceCodec
from epl.version import VERSION


PACK_FORMAT_VERSION = "1"


@dataclass(frozen=True, slots=True)
class TracePack:
    manifest: dict[str, object]
    compressed_payload: bytes


def build_trace_pack(
    *,
    benchmark_result: Mapping[str, object],
    run_id: str,
    source_label: str,
    source_format: str,
) -> TracePack:
    semantic_payload = benchmark_result["semantic_payload"]
    canonical_payload = benchmark_result["canonical_payload"]
    semantic_bytes = _compact_bytes(semantic_payload)
    compressed_payload = zlib.compress(semantic_bytes, level=9)
    manifest = {
        "pack_format_version": PACK_FORMAT_VERSION,
        "product_version": VERSION,
        "run_id": run_id,
        "source": source_label,
        "source_format": source_format,
        "session_count": benchmark_result["summary"]["session_count"],
        "workflow_count": benchmark_result["summary"]["workflow_count"],
        "span_count": benchmark_result["summary"]["span_count"],
        "template_count": benchmark_result["summary"]["template_count"],
        "template_reuse_rate": benchmark_result["summary"]["template_reuse_rate"],
        "raw_json_bytes": benchmark_result["summary"]["raw_json_bytes"],
        "canonical_json_bytes": benchmark_result["summary"]["canonical_json_bytes"],
        "semantic_json_bytes": benchmark_result["summary"]["semantic_json_bytes"],
        "pack_zlib_bytes": len(compressed_payload),
        "pack_ratio_vs_raw": round(len(compressed_payload) / max(int(benchmark_result["summary"]["raw_json_bytes"]), 1), 4),
        "semantic_plus_zlib_gain_vs_raw_zlib": benchmark_result["summary"]["semantic_plus_zlib_gain_vs_raw_zlib"],
        "canonical_sha256": sha256(_compact_bytes(canonical_payload)).hexdigest(),
        "semantic_sha256": sha256(semantic_bytes).hexdigest(),
        "pack_sha256": sha256(compressed_payload).hexdigest(),
        "lossless_canonical_roundtrip": True,
    }
    return TracePack(manifest=manifest, compressed_payload=compressed_payload)


def decode_trace_pack(compressed_payload: bytes) -> list[dict[str, object]]:
    payload = json.loads(zlib.decompress(compressed_payload).decode("utf-8"))
    codec = SemanticTraceCodec()
    return codec.decode_sessions(payload)


def _compact_bytes(payload: object) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
