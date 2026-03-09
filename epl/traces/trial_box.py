from __future__ import annotations

import gzip
import json
import shutil
import time
import zipfile
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any

from epl.logging_utils import ensure_dirs
from epl.traces.archive import run_trace_archive
from epl.traces.corpus import discover_trace_sources, run_trace_corpus
from epl.traces.fit_scoring import build_workload_fit_report
from epl.traces.fixtures import detect_trace_input_format
from epl.traces.partner_trial import _partner_recommendation
from epl.traces.report_branding import load_brand_asset_data_uri
from epl.traces.scorecard import run_trace_scorecard
from epl.traces.verify import verify_trace_artifact
from epl.version import VERSION

try:
    import zstandard as zstandard  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    zstandard = None


def run_trial_box(
    *,
    input_path: Path,
    output_dir: Path = Path("outputs"),
    window_size: int = 6,
    max_active_templates: int | None = 8,
    stream_replay_passes: int = 4,
    retention_candidates: tuple[int | None, ...] | None = None,
) -> dict[str, Any]:
    input_path = Path(input_path)
    output_dir = Path(output_dir)
    trial_root = output_dir / "trial_box"
    ensure_dirs(trial_root)

    run_id = _build_run_id()
    run_dir = trial_root / run_id
    extracted_dir = run_dir / "extracted"
    context_output_dir = run_dir / "context_output"
    ensure_dirs(run_dir, extracted_dir, context_output_dir)

    resolved_input_path, input_kind = _resolve_input(input_path=input_path, extracted_dir=extracted_dir)
    sources = discover_trace_sources(resolved_input_path)
    if not sources:
        raise ValueError("no supported trace sources found for trial box")

    detected_formats = {}
    for source in sources:
        source_format = detect_trace_input_format(source)
        if source_format.startswith("unknown"):
            raise ValueError(f"unsupported trace input format for {source}")
        detected_formats[source.as_posix()] = source_format

    source_bytes = sum(path.stat().st_size for path in sources)
    source_gzip_bytes = sum(_gzip_size(path.read_bytes()) for path in sources)
    source_zstd_bytes = _zstd_total_bytes(sources)

    encode_started = time.perf_counter()
    archive_result = run_trace_archive(
        output_dir=context_output_dir,
        input_path=resolved_input_path,
        window_size=window_size,
        max_active_templates=max_active_templates,
        stream_replay_passes=stream_replay_passes,
        retention_candidates=retention_candidates,
    )
    encode_seconds = time.perf_counter() - encode_started

    verify_started = time.perf_counter()
    verification_result = verify_trace_artifact(Path(str(archive_result["bundle_path"])))
    verify_seconds = time.perf_counter() - verify_started

    corpus_result = run_trace_corpus(
        output_dir=context_output_dir,
        input_path=resolved_input_path,
        window_size=window_size,
        max_active_templates=max_active_templates,
        stream_replay_passes=stream_replay_passes,
        retention_candidates=retention_candidates,
    )
    scorecard_result = run_trace_scorecard(
        output_dir=context_output_dir,
        input_path=resolved_input_path,
        window_size=window_size,
        max_active_templates=max_active_templates,
        stream_replay_passes=stream_replay_passes,
        retention_candidates=retention_candidates,
    )

    archive_gain_bytes = int(archive_result["archive_bundle_gain_vs_source_gzip"])
    replay_verified = bool(verification_result["ok"])
    verdict = _partner_recommendation(
        replay_verified=replay_verified,
        archive_gain_vs_source_gzip=archive_gain_bytes,
        pilot_ready_workflow_count=int(scorecard_result["pilot_ready_workflow_count"]),
        positive_family_count=int(archive_result["positive_archive_gain_source_count"]),
    )
    fit_report = build_workload_fit_report(
        input_path=resolved_input_path,
        archive_gain_vs_source_gzip=archive_gain_bytes,
        source_gzip_total_bytes=source_gzip_bytes,
        recommendation=verdict,
        replay_verified=replay_verified,
        workload_label=_infer_workload_label(input_path=input_path, resolved_input_path=resolved_input_path),
    )

    summary = {
        "version": VERSION,
        "run_id": run_id,
        "context": "trial_box",
        "generated_at": run_id,
        "input_path": str(input_path),
        "resolved_input_path": str(resolved_input_path),
        "input_kind": input_kind,
        "supported_source_count": len(sources),
        "detected_formats": detected_formats,
        "source_bytes": source_bytes,
        "source_gzip_bytes": source_gzip_bytes,
        "source_zstd_bytes": source_zstd_bytes,
        "epl_bundle_bytes": int(archive_result["archive_bundle_bytes"]),
        "archive_gain_vs_source_gzip_bytes": archive_gain_bytes,
        "archive_gain_vs_source_gzip_percent": round(archive_gain_bytes / max(source_gzip_bytes, 1), 4),
        "encode_seconds": round(encode_seconds, 6),
        "verify_seconds": round(verify_seconds, 6),
        "replay_verified": int(replay_verified),
        "reproducibility_verified": int(corpus_result["reproducibility_verified"]),
        "verdict": verdict,
        "fit_label": str(fit_report["fit_label"]),
        "fit_score": int(fit_report["fit_score"]),
        "fit_summary": str(fit_report["human_summary"]),
        "fit_explanation": list(fit_report["explanation"]),
        "fit_metrics": dict(fit_report["raw_metrics"]),
        "source_family_count": int(corpus_result["source_family_count"]),
        "positive_archive_gain_source_count": int(archive_result["positive_archive_gain_source_count"]),
        "negative_archive_gain_source_count": int(archive_result["negative_archive_gain_source_count"]),
        "pilot_ready_workflow_count": int(scorecard_result["pilot_ready_workflow_count"]),
        "workflow_count": int(scorecard_result["workflow_count"]),
        "archive_bundle_path": str(archive_result["bundle_path"]),
        "archive_manifest_path": str(archive_result["manifest_path"]),
        "archive_inventory_path": str(archive_result["inventory_path"]),
    }

    metrics = {
        "baseline_metrics": {
            "source_bytes": source_bytes,
            "source_gzip_bytes": source_gzip_bytes,
            "source_zstd_bytes": source_zstd_bytes,
            "epl_bundle_bytes": int(archive_result["archive_bundle_bytes"]),
            "archive_gain_vs_source_gzip_bytes": archive_gain_bytes,
            "archive_gain_vs_source_gzip_percent": round(archive_gain_bytes / max(source_gzip_bytes, 1), 4),
        },
        "verification": verification_result,
        "fit_report": fit_report,
        "archive_result": archive_result,
        "corpus_result": corpus_result,
        "scorecard_result": scorecard_result,
    }

    summary_json_path = run_dir / "summary.json"
    summary_md_path = run_dir / "summary.md"
    summary_html_path = run_dir / "summary.html"
    metrics_json_path = run_dir / "metrics.json"
    manifest_copy_path = run_dir / "trace_pack_manifest.json"
    verification_json_path = run_dir / "verification.json"

    summary_json_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    summary_md_path.write_text(_build_trial_box_markdown(summary), encoding="utf-8")
    summary_html_path.write_text(_build_trial_box_html(summary), encoding="utf-8")
    metrics_json_path.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
    verification_json_path.write_text(json.dumps(verification_result, indent=2, sort_keys=True), encoding="utf-8")
    shutil.copyfile(Path(str(archive_result["manifest_path"])), manifest_copy_path)

    latest_paths = {
        "latest_summary_json": trial_root / "latest_summary.json",
        "latest_summary_md": trial_root / "latest_summary.md",
        "latest_summary_html": trial_root / "latest_summary.html",
        "latest_metrics_json": trial_root / "latest_metrics.json",
        "latest_manifest_copy": trial_root / "latest_trace_pack_manifest.json",
    }
    for key, target in latest_paths.items():
        source = {
            "latest_summary_json": summary_json_path,
            "latest_summary_md": summary_md_path,
            "latest_summary_html": summary_html_path,
            "latest_metrics_json": metrics_json_path,
            "latest_manifest_copy": manifest_copy_path,
        }[key]
        shutil.copyfile(source, target)

    return {
        **summary,
        "summary_json_path": str(summary_json_path),
        "summary_md_path": str(summary_md_path),
        "summary_html_path": str(summary_html_path),
        "metrics_json_path": str(metrics_json_path),
        "verification_json_path": str(verification_json_path),
        "manifest_copy_path": str(manifest_copy_path),
        **{key: str(value) for key, value in latest_paths.items()},
    }


def _resolve_input(*, input_path: Path, extracted_dir: Path) -> tuple[Path, str]:
    if input_path.is_dir():
        return input_path, "directory"
    if input_path.suffix.lower() == ".zip":
        extract_root = extracted_dir / input_path.stem
        ensure_dirs(extract_root)
        with zipfile.ZipFile(input_path, "r") as archive:
            for member in archive.infolist():
                if member.is_dir():
                    continue
                member_path = Path(member.filename)
                if member_path.suffix.lower() not in {".json", ".jsonl"}:
                    continue
                safe_parts = [part for part in member_path.parts if part not in {"", ".", ".."}]
                target = extract_root.joinpath(*safe_parts)
                ensure_dirs(target.parent)
                with archive.open(member, "r") as source, target.open("wb") as destination:
                    shutil.copyfileobj(source, destination)
        return extract_root, "zip_archive"
    if input_path.suffix.lower() not in {".json", ".jsonl"}:
        raise ValueError(f"unsupported trial-box input: {input_path}")
    return input_path, "single_file"


def _infer_workload_label(*, input_path: Path, resolved_input_path: Path) -> str:
    if resolved_input_path.is_dir():
        return input_path.stem or input_path.name
    metadata_path = resolved_input_path.with_name(f"{resolved_input_path.name}.meta.json")
    if metadata_path.exists():
        try:
            payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = {}
        if isinstance(payload, dict) and payload.get("workload_family"):
            return str(payload["workload_family"])
    return resolved_input_path.stem


def _gzip_size(payload: bytes) -> int:
    return len(gzip.compress(payload, compresslevel=9, mtime=0))


def _zstd_total_bytes(paths: list[Path]) -> int | None:
    if zstandard is None:
        return None
    compressor = zstandard.ZstdCompressor(level=19)
    return sum(len(compressor.compress(path.read_bytes())) for path in paths)


def _build_trial_box_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# EPL Trial Box Summary",
        "",
        f"- Generated: `{summary['generated_at']}`",
        f"- Verdict: `{summary['verdict']}`",
        f"- Workload fit: `{summary['fit_label']}` (score `{summary['fit_score']}`)",
        f"- Replay verified: `{bool(int(summary['replay_verified']))}`",
        "",
        "## Baselines",
        "",
        f"- Source bytes: `{int(summary['source_bytes']):,}`",
        f"- Source + gzip bytes: `{int(summary['source_gzip_bytes']):,}`",
        f"- Source + zstd bytes: `{_format_optional_bytes(summary['source_zstd_bytes'])}`",
        f"- EPL bundle bytes: `{int(summary['epl_bundle_bytes']):,}`",
        f"- Archive gain vs source + gzip: `{int(summary['archive_gain_vs_source_gzip_bytes']):+d}` bytes (`{float(summary['archive_gain_vs_source_gzip_percent']) * 100:.1f}%`)",
        "",
        "## What this means",
        "",
        f"{summary['fit_summary']}",
        "",
        "## Fit explanation",
        "",
    ]
    lines.extend(f"- {item}" for item in summary["fit_explanation"])
    lines.extend(
        [
            "",
            "## Paths",
            "",
            f"- Manifest copy: `{summary['archive_manifest_path']}`",
            f"- Archive bundle: `{summary['archive_bundle_path']}`",
            f"- Inventory: `{summary['archive_inventory_path']}`",
        ]
    )
    return "\n".join(lines) + "\n"


def _build_trial_box_html(summary: dict[str, Any]) -> str:
    report_logo = load_brand_asset_data_uri("helix-trial-report-logo.png")
    company_logo = load_brand_asset_data_uri("trace-integrity-logo.png")
    explanation = "".join(f"<li>{escape(str(item))}</li>" for item in summary["fit_explanation"])
    format_rows = "".join(
        f"<li><code>{escape(path)}</code>: {escape(fmt)}</li>"
        for path, fmt in sorted(summary["detected_formats"].items())
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Helix Trial Report</title>
  <style>
    :root {{
      --bg: #0c1530;
      --panel: #111b35;
      --panel-raised: #172544;
      --ink: #eef3fb;
      --muted: #aab4c8;
      --line: rgba(117, 138, 181, 0.22);
      --accent: #2f67ee;
      --success: #1fbe60;
      --shadow: 0 14px 34px rgba(0, 0, 0, 0.32);
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Inter", system-ui, sans-serif; background: var(--bg); color: var(--ink); }}
    main {{ width: min(1020px, calc(100% - 24px)); margin: 24px auto 32px; }}
    section {{ background: var(--panel); border: 1px solid var(--line); border-radius: 16px; padding: 20px; margin-bottom: 18px; box-shadow: var(--shadow); }}
    h1, h2 {{ font-family: "Geist", "Inter", system-ui, sans-serif; margin: 0 0 12px; }}
    h1 {{ font-size: 2rem; }}
    p, li {{ color: var(--muted); line-height: 1.65; }}
    ul {{ margin: 0; padding-left: 1.2rem; }}
    code {{ font-family: "JetBrains Mono", "Consolas", monospace; background: var(--panel-raised); color: var(--accent); padding: 0.12rem 0.35rem; border-radius: 0.3rem; word-break: break-word; }}
    .nav {{ display: flex; justify-content: space-between; align-items: center; gap: 16px; }}
    .report-logo {{ display: block; width: auto; height: 34px; max-width: min(100%, 320px); }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; }}
    .metric {{ background: linear-gradient(180deg, #142141, #10192f); }}
    .metric strong {{ display: block; font-size: 1.6rem; margin-top: 8px; color: var(--ink); }}
    .mini-label {{ color: var(--muted); font-size: 0.76rem; text-transform: uppercase; letter-spacing: 0.08em; }}
    .footer {{ display: flex; align-items: center; justify-content: space-between; gap: 16px; color: var(--muted); font-size: 0.92rem; }}
    .footer-logo {{ display: block; width: auto; height: 22px; max-width: min(100%, 260px); }}
    .footer-copy {{ color: var(--ink); }}
    @media (max-width: 720px) {{
      main {{ width: min(100% - 20px, 1020px); margin: 14px auto 26px; }}
      .nav, .footer {{ flex-direction: column; align-items: flex-start; }}
    }}
  </style>
</head>
<body>
  <main>
    <section>
      <div class="nav">
        <div>{('<img src="' + report_logo + '" alt="Helix trial report" class="report-logo">') if report_logo else '<h1>Helix Trial Report</h1>'}</div>
        <div>{escape(str(summary["generated_at"]))}</div>
      </div>
      <p style="margin-top: 16px;">This report is the open source Helix trial output: one local run, one workload-fit verdict, one replay check, and one economics comparison against source plus gzip.</p>
      <div class="grid">
        <div class="metric"><span class="mini-label">Verdict</span><strong>{escape(str(summary["verdict"]))}</strong></div>
        <div class="metric"><span class="mini-label">Fit label</span><strong>{escape(str(summary["fit_label"]))}</strong></div>
        <div class="metric"><span class="mini-label">Replay verified</span><strong>{"yes" if int(summary["replay_verified"]) else "no"}</strong></div>
        <div class="metric"><span class="mini-label">Archive gain vs gzip</span><strong>{int(summary["archive_gain_vs_source_gzip_bytes"]):+d}</strong></div>
      </div>
    </section>
    <section>
      <h2>Baselines</h2>
      <ul>
        <li>Source bytes: <code>{int(summary["source_bytes"]):,}</code></li>
        <li>Source + gzip bytes: <code>{int(summary["source_gzip_bytes"]):,}</code></li>
        <li>Source + zstd bytes: <code>{escape(_format_optional_bytes(summary["source_zstd_bytes"]))}</code></li>
        <li>EPL bundle bytes: <code>{int(summary["epl_bundle_bytes"]):,}</code></li>
        <li>Encode time: <code>{float(summary["encode_seconds"]):.4f}s</code></li>
        <li>Verify time: <code>{float(summary["verify_seconds"]):.4f}s</code></li>
      </ul>
    </section>
    <section>
      <h2>Detected formats</h2>
      <ul>{format_rows}</ul>
    </section>
    <section>
      <h2>Fit explanation</h2>
      <p>{escape(str(summary["fit_summary"]))}</p>
      <ul>{explanation}</ul>
    </section>
    <footer class="footer">
      <div>{('<img src="' + company_logo + '" alt="Trace Integrity" class="footer-logo">') if company_logo else 'Trace Integrity'}</div>
      <div class="footer-copy">Helix {escape(VERSION)} · Open source trial</div>
    </footer>
  </main>
</body>
</html>
"""


def _format_optional_bytes(value: int | None) -> str:
    if value is None:
        return "unavailable"
    return f"{int(value):,}"


def _build_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


__all__ = ["run_trial_box"]
