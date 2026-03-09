from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any

from epl.traces.archive import decode_trace_archive_bundle, read_trace_archive_bundle
from epl.traces.package import decode_trace_pack
from epl.traces.streaming import decode_streaming_trace_packs, load_streaming_trace_packs


def verify_trace_artifact(path: Path) -> dict[str, Any]:
    path = Path(path)
    if path.is_dir():
        return verify_stream_directory(path)
    if path.suffix.lower() == ".eplbundle":
        return verify_trace_archive_bundle_file(path)
    return verify_trace_pack_file(path)


def verify_trace_pack_file(payload_path: Path, manifest_path: Path | None = None) -> dict[str, Any]:
    payload_path = Path(payload_path)
    manifest_path = Path(manifest_path) if manifest_path is not None else _infer_manifest_path(payload_path)
    compressed_payload = payload_path.read_bytes()
    decoded_sessions = decode_trace_pack(compressed_payload)
    canonical_bytes = _compact_bytes(decoded_sessions)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    pack_sha = sha256(compressed_payload).hexdigest()
    canonical_sha = sha256(canonical_bytes).hexdigest()
    checks = {
        "pack_sha256_match": pack_sha == str(manifest.get("pack_sha256", "")),
        "canonical_sha256_match": canonical_sha == str(manifest.get("canonical_sha256", "")),
        "lossless_canonical_roundtrip": bool(manifest.get("lossless_canonical_roundtrip", False)),
        "pack_size_match": len(compressed_payload) == int(manifest.get("pack_zlib_bytes", len(compressed_payload))),
    }
    ok = all(checks.values())
    return {
        "artifact_type": "trace_pack",
        "ok": ok,
        "payload_path": str(payload_path),
        "manifest_path": str(manifest_path),
        "session_count": len(decoded_sessions),
        "pack_sha256": pack_sha,
        "canonical_sha256": canonical_sha,
        "checks": checks,
    }


def verify_stream_directory(stream_dir: Path) -> dict[str, Any]:
    stream_dir = Path(stream_dir)
    payload_paths = sorted(stream_dir.glob("window_*_payload.zlib"))
    manifest_paths = sorted(stream_dir.glob("window_*_manifest.json"))
    compressed_payloads = load_streaming_trace_packs(stream_dir)
    decoded_sessions = decode_streaming_trace_packs(compressed_payloads)

    manifest_rows = [json.loads(path.read_text(encoding="utf-8")) for path in manifest_paths]
    expected_session_count = sum(int(row.get("session_count", 0)) for row in manifest_rows)
    checks = {
        "payload_count_matches_manifest_count": len(payload_paths) == len(manifest_paths),
        "decoded_session_count_matches_manifest_total": len(decoded_sessions) == expected_session_count,
        "all_window_gains_reported": all("stream_gain_vs_windowed_raw_zlib" in row for row in manifest_rows),
    }
    ok = all(checks.values())
    return {
        "artifact_type": "stream_directory",
        "ok": ok,
        "stream_dir": str(stream_dir),
        "window_count": len(payload_paths),
        "decoded_session_count": len(decoded_sessions),
        "checks": checks,
    }


def verify_trace_archive_bundle_file(bundle_path: Path) -> dict[str, Any]:
    bundle_path = Path(bundle_path)
    bundle = read_trace_archive_bundle(bundle_path)
    entries = list(bundle["entries"])
    decoded_sessions = decode_trace_archive_bundle(bundle_path)

    entry_checks = []
    payload_size_total = 0
    for entry in entries:
        pack_payload = entry["pack_payload"]
        payload_size_total += len(pack_payload)
        decoded_payload_sessions = decode_trace_pack(pack_payload)
        canonical_sha = sha256(_compact_bytes(decoded_payload_sessions)).hexdigest()
        pack_sha = sha256(pack_payload).hexdigest()
        entry_checks.append(
            {
                "source_name": str(entry.get("source_name", "")),
                "pack_sha256_match": pack_sha == str(entry.get("pack_sha256", "")),
                "canonical_sha256_match": canonical_sha == str(entry.get("canonical_sha256", "")),
                "payload_length_match": len(pack_payload) == int(entry.get("payload_length", len(pack_payload))),
            }
        )

    checks = {
        "archive_bundle_format_version_present": bool(bundle["manifest"].get("archive_bundle_format_version")),
        "payload_count_matches_manifest_count": len(entries) == int(bundle["manifest"].get("source_count", len(entries))),
        "lossless_canonical_roundtrip": bool(bundle["manifest"].get("lossless_canonical_roundtrip", False)),
        "payload_lengths_positive": all(int(entry.get("payload_length", 0)) > 0 for entry in entries),
        "entry_checks_ok": all(all(value for key, value in row.items() if key != "source_name") for row in entry_checks),
        "decoded_session_count_positive": len(decoded_sessions) > 0,
    }
    ok = all(checks.values())
    return {
        "artifact_type": "trace_archive_bundle",
        "ok": ok,
        "bundle_path": str(bundle_path),
        "bundle_sha256": str(bundle["bundle_sha256"]),
        "bundle_size": int(bundle["bundle_size"]),
        "entry_count": len(entries),
        "decoded_session_count": len(decoded_sessions),
        "payload_size_total": payload_size_total,
        "checks": checks,
        "entry_checks": entry_checks,
    }


def _infer_manifest_path(payload_path: Path) -> Path:
    candidate = payload_path.with_name(payload_path.name.replace("_trace_pack.zlib", "_trace_pack_manifest.json"))
    if candidate.exists():
        return candidate
    raise ValueError(f"could not infer manifest path for {payload_path}")


def _compact_bytes(payload: object) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
