from __future__ import annotations

import json
import zlib
from collections.abc import Sequence
from html import escape
from hashlib import sha256
from pathlib import Path
from typing import Any

from epl.analysis.dashboard import refresh_latest_dashboard
from epl.logging_utils import ensure_dirs, write_csv
from epl.traces.benchmark import run_trace_benchmark
from epl.traces.package import decode_trace_pack
from epl.traces.corpus import discover_trace_sources
from epl.traces.extensions import ArchiveManifestContext, get_enterprise_extensions
from epl.version import VERSION
from epl.version_history import current_version_record

ARCHIVE_BUNDLE_FORMAT_VERSION = "1"
ARCHIVE_BUNDLE_MAGIC = b"EPLTRACEARCHIVE1\n"


def run_trace_archive(
    *,
    output_dir: Path = Path("outputs"),
    input_path: Path | None = None,
    window_size: int = 6,
    max_active_templates: int | None = 8,
    stream_replay_passes: int = 4,
    retention_candidates: Sequence[int | None] | None = None,
) -> dict[str, object]:
    output_dir = Path(output_dir)
    archive_dir = output_dir / "archive"
    dashboard_dir = output_dir / "dashboard"
    ensure_dirs(archive_dir, dashboard_dir)

    sources = discover_trace_sources(input_path)
    if not sources:
        raise ValueError("no supported trace sources found")

    source_rows: list[dict[str, object]] = []
    source_results: list[dict[str, object]] = []
    for index, source in enumerate(sources, start=1):
        slug = f"{index:02d}_{_slugify(source.stem)}"
        source_output_dir = archive_dir / slug
        result = run_trace_benchmark(
            output_dir=source_output_dir,
            input_path=source,
            window_size=window_size,
            max_active_templates=max_active_templates,
            stream_replay_passes=stream_replay_passes,
            retention_candidates=retention_candidates,
        )
        source_rows.append(
            {
                "source_name": source.name,
                "source_path": source.as_posix(),
                "source_format": result["source_format"],
                "source_family": result["source_family"],
                "session_count": int(result["session_count"]),
                "span_count": int(result["span_count"]),
                "source_bytes": int(result["source_bytes"]),
                "source_gzip_bytes": int(result["source_gzip_bytes"]),
                "raw_json_bytes": int(result["raw_json_bytes"]),
                "zlib_raw_bytes": int(result["zlib_raw_bytes"]),
                "pack_zlib_bytes": int(result["pack_zlib_bytes"]),
                "stream_pack_total_bytes": int(result["stream_pack_total_bytes"]),
                "stream_pack_gain_vs_windowed_raw_zlib": int(result["stream_pack_gain_vs_windowed_raw_zlib"]),
                "recommended_max_active_templates": str(result["recommended_max_active_templates"]),
                "recommended_gain_delta_vs_baseline": int(result["recommended_gain_delta_vs_baseline"]),
                "pack_manifest_path": Path(result["pack_manifest_path"]).as_posix(),
                "pack_payload_path": Path(result["pack_payload_path"]).as_posix(),
                "templates_path": Path(result["templates_path"]).as_posix(),
                "preview_path": Path(result["preview_path"]).as_posix(),
                "report_path": Path(result["report_path"]).relative_to(output_dir).as_posix(),
            }
        )
        source_results.append(result)

    run_id = source_results[-1]["run_id"]
    manifest_path = archive_dir / f"{run_id}_trace_archive_manifest.json"
    inventory_path = archive_dir / f"{run_id}_trace_archive_inventory.csv"
    bundle_path = archive_dir / f"{run_id}_trace_archive_bundle.eplbundle"
    report_path = dashboard_dir / f"{run_id}_trace_archive.html"
    latest_report_path = dashboard_dir / "latest_trace_archive.html"

    archive_manifest, bundle_payload, attributed_bundle_bytes = _build_archive_bundle(
        source_rows=source_rows,
        run_id=run_id,
        output_dir=output_dir,
        window_size=window_size,
        max_active_templates=max_active_templates,
        stream_replay_passes=stream_replay_passes,
    )
    for row, attributed_bytes in zip(source_rows, attributed_bundle_bytes):
        row["source_archive_bundle_bytes"] = int(attributed_bytes)
    manifest_path.write_text(json.dumps(archive_manifest, indent=2, sort_keys=True), encoding="utf-8")
    write_csv(inventory_path, source_rows)
    bundle_path.write_bytes(bundle_payload)

    summary = _build_archive_summary(
        source_rows=source_rows,
        bundle_path=bundle_path,
        manifest_path=manifest_path,
        inventory_path=inventory_path,
        archive_manifest=archive_manifest,
        max_active_templates=max_active_templates,
        stream_replay_passes=stream_replay_passes,
    )
    summary_path = archive_dir / f"{run_id}_trace_archive_summary.csv"
    write_csv(summary_path, [summary])

    html = _build_archive_report_html(summary=summary, rows=source_rows, bundle_path=bundle_path, manifest_path=manifest_path, inventory_path=inventory_path)
    report_path.write_text(html, encoding="utf-8")
    latest_report_path.write_text(html, encoding="utf-8")
    refresh_latest_dashboard(output_dir)

    return {
        "version": VERSION,
        "run_id": run_id,
        **summary,
        "source_rows": source_rows,
        "summary_path": str(summary_path),
        "manifest_path": str(manifest_path),
        "inventory_path": str(inventory_path),
        "bundle_path": str(bundle_path),
        "report_path": str(report_path),
        "latest_report_path": str(latest_report_path),
        "compliance": archive_manifest.get("compliance", {}),
    }


def _build_archive_summary(
    *,
    source_rows: Sequence[dict[str, object]],
    bundle_path: Path,
    manifest_path: Path,
    inventory_path: Path,
    archive_manifest: dict[str, object],
    max_active_templates: int | None,
    stream_replay_passes: int,
) -> dict[str, object]:
    raw_total = sum(int(row["raw_json_bytes"]) for row in source_rows)
    raw_zlib_total = sum(int(row["zlib_raw_bytes"]) for row in source_rows)
    source_gzip_total = sum(int(row["source_gzip_bytes"]) for row in source_rows)
    pack_total = sum(int(row["pack_zlib_bytes"]) for row in source_rows)
    stream_total = sum(int(row["stream_pack_total_bytes"]) for row in source_rows)
    bundle_bytes = bundle_path.stat().st_size
    source_archive_bundle_total = sum(int(row["source_archive_bundle_bytes"]) for row in source_rows)
    avg_tuning_uplift = sum(int(row["recommended_gain_delta_vs_baseline"]) for row in source_rows) / max(len(source_rows), 1)
    recommended_cap = _recommend_archive_cap(source_rows)
    family_gains = _archive_family_gains(source_rows)
    positive_archive_gain_source_count = sum(gain > 0 for gain in family_gains.values())
    negative_archive_gain_source_count = sum(gain <= 0 for gain in family_gains.values())
    return {
        "source_count": len(source_rows),
        "source_family_count": len({str(row["source_family"]) for row in source_rows}),
        "raw_json_total_bytes": raw_total,
        "raw_zlib_total_bytes": raw_zlib_total,
        "source_gzip_total_bytes": source_gzip_total,
        "pack_zlib_total_bytes": pack_total,
        "stream_pack_total_bytes": stream_total,
        "archive_bundle_bytes": bundle_bytes,
        "source_archive_bundle_total_bytes": source_archive_bundle_total,
        "archive_bundle_gain_vs_raw_zlib": raw_zlib_total - bundle_bytes,
        "archive_bundle_gain_vs_source_gzip": source_gzip_total - bundle_bytes,
        "positive_archive_gain_source_count": positive_archive_gain_source_count,
        "negative_archive_gain_source_count": negative_archive_gain_source_count,
        "semantic_pack_gain_vs_raw_zlib": raw_zlib_total - pack_total,
        "recommended_max_active_templates": recommended_cap,
        "avg_recommended_gain_delta_vs_baseline": round(avg_tuning_uplift, 2),
        "max_active_templates": "unbounded" if max_active_templates is None else max_active_templates,
        "stream_replay_passes": stream_replay_passes,
        "replay_verified": int(bool(archive_manifest.get("lossless_canonical_roundtrip", False))),
        "bundle_sha256": sha256(bundle_path.read_bytes()).hexdigest(),
        "manifest_sha256": sha256(manifest_path.read_bytes()).hexdigest(),
        "inventory_sha256": sha256(inventory_path.read_bytes()).hexdigest(),
    }


def _build_archive_bundle(
    *,
    source_rows: Sequence[dict[str, object]],
    run_id: str,
    output_dir: Path,
    window_size: int,
    max_active_templates: int | None,
    stream_replay_passes: int,
) -> tuple[dict[str, object], bytes, list[int]]:
    archive_entries: list[dict[str, object]] = []
    bundle_entries: list[dict[str, object]] = []
    payloads: list[bytes] = []
    roundtrip_verified = True
    for row in source_rows:
        pack_manifest_path = Path(str(row["pack_manifest_path"]))
        pack_payload_path = Path(str(row["pack_payload_path"]))
        pack_manifest = json.loads(pack_manifest_path.read_text(encoding="utf-8"))
        payload_bytes = pack_payload_path.read_bytes()
        roundtrip_verified = roundtrip_verified and bool(pack_manifest["lossless_canonical_roundtrip"])
        archive_entries.append(
            {
                "source_name": str(row["source_name"]),
                "source_format": str(row["source_format"]),
                "source_family": str(row["source_family"]),
                "payload_length": len(payload_bytes),
                "session_count": int(row["session_count"]),
                "workflow_count": int(pack_manifest["workflow_count"]),
                "span_count": int(row["span_count"]),
                "canonical_sha256": str(pack_manifest["canonical_sha256"]),
                "pack_sha256": str(pack_manifest["pack_sha256"]),
            }
        )
        bundle_entries.append(
            {
                "source_name": str(row["source_name"]),
                "payload_length": len(payload_bytes),
                "session_count": int(row["session_count"]),
                "canonical_sha256": str(pack_manifest["canonical_sha256"]),
                "pack_sha256": str(pack_manifest["pack_sha256"]),
            }
        )
        payloads.append(payload_bytes)

    archive_manifest = {
        "archive_bundle_format_version": ARCHIVE_BUNDLE_FORMAT_VERSION,
        "product_version": VERSION,
        "run_id": run_id,
        "source_count": len(source_rows),
        "window_size": window_size,
        "max_active_templates": "unbounded" if max_active_templates is None else max_active_templates,
        "stream_replay_passes": stream_replay_passes,
        "lossless_canonical_roundtrip": roundtrip_verified,
        "bundle_manifest_encoding": "zlib+compact_json",
        "bundle_payload_contract": "compressed_manifest_plus_concatenated_trace_pack_payloads",
        "review_artifacts_excluded_from_bundle": [
            "trace_preview.json",
            "trace_benchmark.html",
            "trace_archive.html",
            "trace_archive_inventory.csv",
        ],
        "entries": archive_entries,
    }
    archive_manifest = get_enterprise_extensions().enrich_archive_manifest(
        archive_manifest=archive_manifest,
        context=ArchiveManifestContext(
            run_id=run_id,
            output_dir=output_dir,
            source_rows=list(source_rows),
            window_size=window_size,
            max_active_templates=max_active_templates,
            stream_replay_passes=stream_replay_passes,
        ),
    )
    bundle_manifest = {
        "archive_bundle_format_version": ARCHIVE_BUNDLE_FORMAT_VERSION,
        "product_version": VERSION,
        "run_id": run_id,
        "source_count": len(source_rows),
        "lossless_canonical_roundtrip": roundtrip_verified,
        "bundle_manifest_encoding": "zlib+compact_json",
        "entries": bundle_entries,
    }
    manifest_bytes = zlib.compress(_compact_json_bytes(bundle_manifest), level=9)
    bundle_payload = ARCHIVE_BUNDLE_MAGIC + len(manifest_bytes).to_bytes(8, byteorder="big") + manifest_bytes + b"".join(payloads)
    attributed_bundle_bytes = _attribute_bundle_bytes(
        source_count=len(bundle_entries),
        bundle_size=len(bundle_payload),
        payload_lengths=[len(payload) for payload in payloads],
        entry_sizes=[len(_compact_json_bytes(entry)) for entry in bundle_entries],
    )
    return archive_manifest, bundle_payload, attributed_bundle_bytes


def read_trace_archive_bundle(bundle_path: Path) -> dict[str, Any]:
    bundle_path = Path(bundle_path)
    payload = bundle_path.read_bytes()
    if not payload.startswith(ARCHIVE_BUNDLE_MAGIC):
        raise ValueError(f"not an EPL archive bundle: {bundle_path}")
    manifest_length_offset = len(ARCHIVE_BUNDLE_MAGIC)
    manifest_length = int.from_bytes(payload[manifest_length_offset:manifest_length_offset + 8], byteorder="big")
    manifest_start = manifest_length_offset + 8
    manifest_end = manifest_start + manifest_length
    compressed_manifest = payload[manifest_start:manifest_end]
    manifest = json.loads(zlib.decompress(compressed_manifest).decode("utf-8"))
    entries = list(manifest.get("entries", []))
    cursor = manifest_end
    payload_entries: list[dict[str, Any]] = []
    for entry in entries:
        payload_length = int(entry["payload_length"])
        pack_payload = payload[cursor:cursor + payload_length]
        payload_entries.append(
            {
                **entry,
                "payload_offset": cursor,
                "payload_length": payload_length,
                "pack_payload": pack_payload,
            }
        )
        cursor += payload_length
    return {
        "manifest": manifest,
        "entries": payload_entries,
        "bundle_sha256": sha256(payload).hexdigest(),
        "bundle_size": len(payload),
    }


def decode_trace_archive_bundle(bundle_path: Path) -> list[dict[str, object]]:
    bundle = read_trace_archive_bundle(bundle_path)
    decoded_sessions: list[dict[str, object]] = []
    for entry in bundle["entries"]:
        decoded_sessions.extend(decode_trace_pack(entry["pack_payload"]))
    return decoded_sessions


def _attribute_bundle_bytes(
    *,
    source_count: int,
    bundle_size: int,
    payload_lengths: Sequence[int],
    entry_sizes: Sequence[int],
) -> list[int]:
    if source_count == 0:
        return []
    shared_bytes = bundle_size - sum(payload_lengths) - sum(entry_sizes)
    base_share, remainder = divmod(shared_bytes, source_count)
    attributed: list[int] = []
    for index, (payload_length, entry_size) in enumerate(zip(payload_lengths, entry_sizes)):
        attributed.append(int(payload_length) + int(entry_size) + base_share + (1 if index < remainder else 0))
    return attributed


def _compact_json_bytes(payload: object) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _build_archive_report_html(
    *,
    summary: dict[str, object],
    rows: Sequence[dict[str, object]],
    bundle_path: Path,
    manifest_path: Path,
    inventory_path: Path,
) -> str:
    current_release = current_version_record()
    row_html = "\n".join(
        "<tr>"
        f"<td><code>{escape(str(row['source_name']))}</code></td>"
        f"<td><code>{escape(str(row['source_format']))}</code></td>"
        f"<td><code>{escape(str(row['source_family']))}</code></td>"
        f"<td>{int(row['session_count'])}</td>"
        f"<td>{int(row['span_count'])}</td>"
        f"<td>{int(row['pack_zlib_bytes'])}</td>"
        f"<td>{int(row['source_gzip_bytes'])}</td>"
        f"<td>{int(row['source_archive_bundle_bytes'])}</td>"
        f"<td>{int(row['source_gzip_bytes']) - int(row['source_archive_bundle_bytes']):+d}</td>"
        f"<td>{int(row['stream_pack_gain_vs_windowed_raw_zlib']):+d}</td>"
        f"<td><code>{escape(str(row['recommended_max_active_templates']))}</code></td>"
        f"<td>{int(row['recommended_gain_delta_vs_baseline']):+d}</td>"
        f"<td><a href=\"../{escape(str(row['report_path']))}\">open report</a></td>"
        "</tr>"
        for row in rows
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="10">
  <title>Trace Archive Adapter Report</title>
  <style>
    :root {{
      --bg: #f4ecdf;
      --panel: rgba(255, 251, 245, 0.94);
      --ink: #182320;
      --muted: #5d655f;
      --line: rgba(24, 35, 32, 0.12);
      --accent: #b85b30;
      --shadow: 0 18px 40px rgba(24, 35, 32, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Aptos", "Segoe UI Variable", "Segoe UI", sans-serif; color: var(--ink); background: linear-gradient(180deg, #f7f0e5 0%, #ece0cf 100%); }}
    .page {{ width: min(1280px, calc(100% - 24px)); margin: 18px auto 32px; }}
    .hero, .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 24px; box-shadow: var(--shadow); }}
    .hero {{ padding: 26px; background: linear-gradient(135deg, #132d29, #3b6e63); color: #f9f5ef; }}
    h1, h2, h3 {{ font-family: "Iowan Old Style", Georgia, serif; margin: 0; }}
    .hero p {{ color: rgba(249, 245, 239, 0.88); line-height: 1.6; max-width: 880px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-top: 18px; }}
    .card {{ padding: 18px; min-width: 0; }}
    .metric span {{ display: block; font-size: 0.76rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); }}
    .metric strong {{ display: block; margin-top: 8px; font-size: 1.8rem; color: var(--ink); }}
    .section {{ margin-top: 20px; }}
    .section p {{ color: var(--muted); line-height: 1.55; }}
    .table-shell {{ overflow-x: auto; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 980px; }}
    th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid var(--line); vertical-align: top; }}
    th {{ font-size: 0.76rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); }}
    code {{ font-family: "Consolas", monospace; background: rgba(24, 35, 32, 0.06); padding: 0.12rem 0.35rem; border-radius: 0.3rem; word-break: break-word; }}
    a {{ color: var(--accent); text-decoration: none; font-weight: 700; }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <p>Helix | v{escape(VERSION)} | {escape(current_release.code_name)}</p>
      <h1>Trace Archive Adapter Report</h1>
      <p>This report packages semantic trace packs into a single replay bundle so a design partner can benchmark storage-path savings without replacing their collector or observability backend. Review collateral stays outside the bundle, so the byte claim only charges what a replayable archive actually needs.</p>
    </section>

    <section class="section">
      <div class="grid">
        <article class="card metric"><span>Sources</span><strong>{int(summary["source_count"])}</strong></article>
        <article class="card metric"><span>Raw zlib Total</span><strong>{int(summary["raw_zlib_total_bytes"])}</strong></article>
        <article class="card metric"><span>Source Gzip Total</span><strong>{int(summary["source_gzip_total_bytes"])}</strong></article>
        <article class="card metric"><span>Pack Total</span><strong>{int(summary["pack_zlib_total_bytes"])}</strong></article>
        <article class="card metric"><span>Bundle Bytes</span><strong>{int(summary["archive_bundle_bytes"])}</strong></article>
        <article class="card metric"><span>Bundle Gain Vs Raw zlib</span><strong>{int(summary["archive_bundle_gain_vs_raw_zlib"]):+d}</strong></article>
        <article class="card metric"><span>Bundle Gain Vs Source Gzip</span><strong>{int(summary["archive_bundle_gain_vs_source_gzip"]):+d}</strong></article>
        <article class="card metric"><span>Positive Families</span><strong>{int(summary["positive_archive_gain_source_count"])}</strong></article>
        <article class="card metric"><span>Negative Families</span><strong>{int(summary["negative_archive_gain_source_count"])}</strong></article>
        <article class="card metric"><span>Recommended Active Cap</span><strong>{escape(str(summary["recommended_max_active_templates"]))}</strong></article>
        <article class="card metric"><span>Replay Passes</span><strong>{int(summary["stream_replay_passes"])}</strong></article>
      </div>
    </section>

    <section class="section">
      <h2>How To Use This</h2>
      <div class="grid">
        <article class="card"><h3>Storage Benchmark</h3><p>Point this adapter at exported OTLP JSON or JSONL trace dumps and compare the replay bundle size against your current raw export plus commodity gzip path. The release question is which source families save bytes and which do not.</p></article>
        <article class="card"><h3>Recommended Cap</h3><p>The archive surfaces a single recommended active-template cap based on the same replay-stress retention tuning used elsewhere in the product.</p></article>
        <article class="card"><h3>Bundle Contract</h3><p>The replay bundle is a compressed compact manifest plus concatenated semantic pack payloads. Preview JSON, HTML reports, and inventory CSV stay outside the bundle and are not charged to the storage-path economics.</p><p><code>{escape(bundle_path.as_posix())}</code></p><p><code>{escape(manifest_path.as_posix())}</code></p><p><code>{escape(inventory_path.as_posix())}</code></p></article>
      </div>
    </section>

    <section class="section">
      <h2>Archive Detail</h2>
      <p>Use this table to see how each source contributes to the archive economics, which families are net-positive, and which retention cap was recommended for each source. The member-byte column is the source's attributed share of the shared replay bundle.</p>
      <div class="card">
        <div class="table-shell">
          <table>
            <thead>
              <tr><th>Source</th><th>Format</th><th>Family</th><th>Sessions</th><th>Spans</th><th>Pack Bytes</th><th>Source Gzip</th><th>Bundle Member Bytes</th><th>Archive Gain</th><th>Stream Gain</th><th>Recommended Cap</th><th>Tuning Uplift</th><th>Report</th></tr>
            </thead>
            <tbody>
              {row_html}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  </main>
</body>
</html>
"""


def _recommend_archive_cap(rows: Sequence[dict[str, object]]) -> str:
    if not rows:
        return "unknown"
    scores: dict[str, tuple[int, int]] = {}
    for row in rows:
        cap = str(row["recommended_max_active_templates"])
        count, uplift_total = scores.get(cap, (0, 0))
        scores[cap] = (count + 1, uplift_total + int(row["recommended_gain_delta_vs_baseline"]))
    ranked = sorted(
        scores.items(),
        key=lambda item: (
            item[1][0],
            item[1][1],
            -(10**9 if item[0] == "unbounded" else int(item[0])),
        ),
        reverse=True,
    )
    return ranked[0][0]


def _archive_family_gains(rows: Sequence[dict[str, object]]) -> dict[str, int]:
    gains: dict[str, int] = {}
    for row in rows:
        family = str(row["source_family"])
        gain = int(row["source_gzip_bytes"]) - int(row["source_archive_bundle_bytes"])
        gains[family] = gains.get(family, 0) + gain
    return gains


def _slugify(value: str) -> str:
    cleaned = []
    for char in value.lower():
        cleaned.append(char if char.isalnum() else "_")
    return "".join(cleaned).strip("_") or "source"
