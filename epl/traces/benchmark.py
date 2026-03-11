import json
import gzip
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from html import escape
from pathlib import Path

from epl.logging_utils import ensure_dirs, write_csv, write_jsonl
from epl.analysis.dashboard import refresh_latest_dashboard
from epl.traces.codec import benchmark_trace_codec
from epl.traces.fixtures import (
    build_trace_fixture_sessions,
    detect_trace_input_format,
    load_trace_sessions,
)
from epl.traces.package import build_trace_pack
from epl.traces.preview import build_safe_trace_preview
from epl.traces.schema import TraceSession
from epl.traces.streaming import build_streaming_trace_packs, write_streaming_trace_packs
from epl.traces.tuning import (
    build_replayed_stream_sessions,
    normalize_retention_candidates,
    tune_stream_retention,
)
from epl.version import VERSION
from epl.version_history import VERSION_HISTORY, current_version_record


def run_trace_benchmark(
    *,
    output_dir: Path = Path("outputs"),
    session_count: int = 18,
    input_path: Path | None = None,
    window_size: int = 6,
    max_active_templates: int | None = 8,
    stream_replay_passes: int = 4,
    retention_candidates: Sequence[int | None] | None = None,
) -> dict[str, object]:
    output_dir = Path(output_dir)
    trace_dir = output_dir / "trace"
    dashboard_dir = output_dir / "dashboard"
    ensure_dirs(trace_dir, dashboard_dir)

    if input_path is not None:
        source_format = detect_trace_input_format(input_path)
        sessions = load_trace_sessions(input_path, input_format=source_format)
        source_label = f"{source_format}:{Path(input_path).as_posix()}"
        source_bytes = Path(input_path).read_bytes()
    else:
        sessions = build_trace_fixture_sessions(session_count=session_count)
        source_format = "built_in_fixtures"
        source_label = "built_in_openinference_fixtures"
        source_bytes = json.dumps([session.to_dict() for session in sessions], sort_keys=True, separators=(",", ":")).encode("utf-8")

    benchmark = benchmark_trace_codec(sessions)
    preview = build_safe_trace_preview(sessions)
    normalized_candidates = normalize_retention_candidates(retention_candidates, baseline_cap=max_active_templates)
    stream_sessions = build_replayed_stream_sessions(sessions, replay_passes=stream_replay_passes)
    stream_benchmark = build_streaming_trace_packs(
        stream_sessions,
        window_size=window_size,
        max_active_templates=max_active_templates,
    )
    retention_tuning = tune_stream_retention(
        sessions,
        window_size=window_size,
        replay_passes=stream_replay_passes,
        candidate_caps=normalized_candidates,
        baseline_cap=max_active_templates,
    )
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    pack = build_trace_pack(
        benchmark_result=benchmark,
        run_id=run_id,
        source_label=source_label,
        source_format=source_format,
    )
    source_gzip_bytes = len(gzip.compress(source_bytes, compresslevel=9, mtime=0))
    source_family = _classify_source_family(
        input_path=input_path,
        source_format=source_format,
        sessions=sessions,
    )
    summary = {
        **dict(benchmark["summary"]),
        "version": VERSION,
        "source": source_label,
        "source_format": source_format,
        "source_family": source_family,
        "source_bytes": len(source_bytes),
        "source_gzip_bytes": source_gzip_bytes,
        "pack_zlib_bytes": pack.manifest["pack_zlib_bytes"],
        "pack_ratio_vs_raw": pack.manifest["pack_ratio_vs_raw"],
        "semantic_pack_gain_vs_source_gzip": source_gzip_bytes - pack.manifest["pack_zlib_bytes"],
        "preview_redacted_field_count": preview["redacted_field_count"],
        "preview_session_count": preview["preview_session_count"],
        "preview_span_count": preview["preview_span_count"],
        "stream_replay_passes": stream_replay_passes,
        "stream_replay_session_count": len(stream_sessions),
        **stream_benchmark["summary"],
        **retention_tuning["summary"],
    }

    raw_sessions_path = trace_dir / f"{run_id}_trace_sessions.jsonl"
    encoded_path = trace_dir / f"{run_id}_semantic_trace_codec.json"
    summary_path = trace_dir / f"{run_id}_trace_summary.csv"
    templates_path = trace_dir / f"{run_id}_trace_templates.csv"
    stream_summary_path = trace_dir / f"{run_id}_trace_stream_windows.csv"
    tuning_path = trace_dir / f"{run_id}_trace_retention_tuning.csv"
    pack_manifest_path = trace_dir / f"{run_id}_trace_pack_manifest.json"
    pack_payload_path = trace_dir / f"{run_id}_trace_pack.zlib"
    preview_path = trace_dir / f"{run_id}_trace_preview.json"
    stream_dir = trace_dir / f"{run_id}_stream_packs"
    report_path = dashboard_dir / f"{run_id}_trace_benchmark.html"
    latest_report_path = dashboard_dir / "latest_trace_benchmark.html"
    version_history_path = dashboard_dir / "version_history.html"

    raw_rows = [
        {
            "session_id": session.session_id,
            "workflow": session.workflow,
            "span_count": len(session.spans),
            "payload": session.to_dict(),
        }
        for session in sessions
    ]
    write_jsonl(raw_sessions_path, raw_rows)
    encoded_path.write_text(_pretty_json(benchmark["semantic_payload"]), encoding="utf-8")
    write_csv(summary_path, [summary])
    write_csv(templates_path, _flatten_template_rows(benchmark["template_rows"]))
    write_csv(stream_summary_path, stream_benchmark["window_rows"])
    write_csv(tuning_path, retention_tuning["candidate_rows"])
    pack_manifest_path.write_text(_pretty_json(pack.manifest), encoding="utf-8")
    pack_payload_path.write_bytes(pack.compressed_payload)
    preview_path.write_text(_pretty_json(preview), encoding="utf-8")
    stream_artifact_rows = write_streaming_trace_packs(stream_dir, stream_benchmark["window_packs"])

    html = _build_trace_report_html(
        sessions=sessions,
        summary=summary,
        manifest=pack.manifest,
        preview=preview,
        stream_summary=stream_benchmark["summary"],
        stream_rows=stream_benchmark["window_rows"],
        tuning_summary=retention_tuning["summary"],
        tuning_rows=retention_tuning["candidate_rows"],
        stream_artifact_rows=stream_artifact_rows,
        workflow_rows=benchmark["workflow_rows"],
        template_rows=benchmark["template_rows"],
        raw_sessions_path=raw_sessions_path,
        encoded_path=encoded_path,
        summary_path=summary_path,
        templates_path=templates_path,
        stream_summary_path=stream_summary_path,
        tuning_path=tuning_path,
        pack_manifest_path=pack_manifest_path,
        pack_payload_path=pack_payload_path,
        preview_path=preview_path,
        stream_dir=stream_dir,
        input_path=input_path,
    )
    version_history_path.write_text(_build_trace_version_history_html(), encoding="utf-8")
    report_path.write_text(html, encoding="utf-8")
    latest_report_path.write_text(html, encoding="utf-8")
    refresh_latest_dashboard(output_dir)

    return {
        "run_id": run_id,
        **summary,
        "raw_sessions_path": str(raw_sessions_path),
        "encoded_path": str(encoded_path),
        "summary_path": str(summary_path),
        "templates_path": str(templates_path),
        "stream_summary_path": str(stream_summary_path),
        "tuning_path": str(tuning_path),
        "pack_manifest_path": str(pack_manifest_path),
        "pack_payload_path": str(pack_payload_path),
        "preview_path": str(preview_path),
        "stream_dir": str(stream_dir),
        "report_path": str(report_path),
        "latest_report_path": str(latest_report_path),
        "version_history_path": str(version_history_path),
    }


def _classify_source_family(
    *,
    input_path: Path | None,
    source_format: str,
    sessions: Sequence[TraceSession],
) -> str:
    if input_path is not None:
        path_text = input_path.stem.lower()
        if "openinference" in path_text:
            return "openinference_ai_trace"
        if "langfuse" in path_text:
            return "langfuse_trace_export"
        if "demo" in path_text or "checkout" in path_text:
            return "otel_demo_trace"
        if "handoff" in path_text:
            return "agent_handoff_trace"
    if any(
        "openinference.span.kind" in span.attributes
        for session in sessions
        for span in session.spans
    ):
        return "openinference_ai_trace"
    if source_format == "jsonl_span_rows":
        return "flat_jsonl_span_export"
    if source_format == "json_span_array":
        return "json_span_array_export"
    if source_format == "otlp_json":
        return "otlp_json_export"
    return "built_in_fixture_trace"


def _flatten_template_rows(template_rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    flattened: list[dict[str, object]] = []
    for row in template_rows:
        flattened.append(
            {
                "template_id": row["template_id"],
                "name": row["name"],
                "kind": row["kind"],
                "status": row["status"],
                "uses": row["uses"],
                "attribute_keys": ", ".join(str(key) for key in row["attribute_keys"]),
                "event_specs": ", ".join(
                    f"{spec['name']}[{', '.join(spec['attribute_keys'])}]"
                    for spec in row["event_specs"]
                ),
            }
        )
    return flattened


def _build_trace_report_html(
    *,
    sessions: Sequence[TraceSession],
    summary: Mapping[str, object],
    manifest: Mapping[str, object],
    preview: Mapping[str, object],
    stream_summary: Mapping[str, object],
    stream_rows: Sequence[Mapping[str, object]],
    tuning_summary: Mapping[str, object],
    tuning_rows: Sequence[Mapping[str, object]],
    stream_artifact_rows: Sequence[Mapping[str, object]],
    workflow_rows: Sequence[Mapping[str, object]],
    template_rows: Sequence[Mapping[str, object]],
    raw_sessions_path: Path,
    encoded_path: Path,
    summary_path: Path,
    templates_path: Path,
    stream_summary_path: Path,
    tuning_path: Path,
    pack_manifest_path: Path,
    pack_payload_path: Path,
    preview_path: Path,
    stream_dir: Path,
    input_path: Path | None,
) -> str:
    current_release = current_version_record()
    workflow_cards = "\n".join(
        (
            '<article class="metric-card soft-card">'
            f'<span>{escape(str(row["workflow"]))}</span>'
            f'<strong>{int(row["session_count"])}</strong>'
            '<p>Trace sessions in this workflow family.</p>'
            "</article>"
        )
        for row in workflow_rows
    )
    template_table_rows = "\n".join(
        "<tr>"
        f"<td>{int(row['template_id'])}</td>"
        f"<td><code>{escape(str(row['name']))}</code></td>"
        f"<td>{escape(str(row['kind']))}</td>"
        f"<td>{int(row['uses'])}</td>"
        f"<td><code>{escape(', '.join(str(key) for key in row['attribute_keys']))}</code></td>"
        "</tr>"
        for row in template_rows[:12]
    )
    if not template_table_rows:
        template_table_rows = '<tr><td colspan="5">No templates generated.</td></tr>'

    preview_rows = []
    for session in preview["sessions"]:
        for span in session["spans"]:
            preview_rows.append(
                "<tr>"
                f"<td><code>{escape(str(session['workflow']))}</code></td>"
                f"<td><code>{escape(str(span['name']))}</code></td>"
                f"<td><code>{escape(str(span['kind']))}</code></td>"
                f"<td><code>{escape(_preview_attribute_summary(span['attributes']))}</code></td>"
                "</tr>"
            )
    preview_table_rows = "\n".join(preview_rows[:10]) or '<tr><td colspan="4">No preview rows available.</td></tr>'
    stream_table_rows = "\n".join(
        "<tr>"
        f"<td>{int(row['window_index'])}</td>"
        f"<td>{int(row['session_count'])}</td>"
        f"<td>{int(row['span_count'])}</td>"
        f"<td>{int(row['new_template_count'])}</td>"
        f"<td>{int(row['reintroduced_template_count'])}</td>"
        f"<td>{int(row['evicted_template_count'])}</td>"
        f"<td>{float(row['reused_span_rate']) * 100:.1f}%</td>"
        f"<td>{int(row['stream_pack_bytes'])}</td>"
        f"<td>{int(row['stream_gain_vs_windowed_raw_zlib']):+d}</td>"
        "</tr>"
        for row in stream_rows
    ) or '<tr><td colspan="9">No stream windows generated.</td></tr>'
    stream_artifact_cards = "\n".join(
        _artifact_card(
            f"Stream Window {int(row['window_index'])}",
            row["payload_path"],
            f"Incremental payload for window {int(row['window_index'])} with matching manifest at {row['manifest_path']}.",
        )
        for row in stream_artifact_rows[:6]
    )
    tuning_table_rows = "\n".join(
        "<tr>"
        f"<td><code>{escape(str(row['candidate_max_active_templates']))}</code></td>"
        f"<td>{'recommended' if int(row['is_recommended']) else ('baseline' if int(row['is_baseline']) else 'candidate')}</td>"
        f"<td>{int(row['stream_pack_gain_vs_windowed_raw_zlib']):+d}</td>"
        f"<td>{int(row['gain_delta_vs_baseline']):+d}</td>"
        f"<td>{float(row['cross_window_template_reuse_rate']) * 100:.1f}%</td>"
        f"<td>{int(row['final_active_template_count'])}</td>"
        f"<td>{int(row['stream_evicted_template_total'])}</td>"
        f"<td>{int(row['stream_reintroduced_template_total'])}</td>"
        "</tr>"
        for row in tuning_rows
    ) or '<tr><td colspan="8">No retention tuning rows generated.</td></tr>'
    prototype_cards = "\n".join(
        [
            _artifact_card(
                "Prototype 1 | Gateway Sidecar",
                "run_trace_gateway.py",
                "Deploy the OTLP gateway beside an existing agent service, then apply the recommended active-template cap to keep stream memory bounded while reports refresh automatically.",
            ),
            _artifact_card(
                "Prototype 2 | Cold Storage Pre-Pack",
                "run_trace_corpus.py",
                "Run exported trace directories through the corpus runner before storage to validate whether semantic packs keep beating raw JSON plus zlib across a broader workload set.",
            ),
            _artifact_card(
                "Prototype 3 | Replay And Audit",
                "run_trace_unpack.py",
                "Use the pack manifest and unpack path as a replay surface for debugging, audit, or cross-agent handoff review without keeping the raw verbose JSON online forever.",
            ),
        ]
    )

    source_description = (
        "Built-in OpenTelemetry/OpenInference-style fixture traces"
        if input_path is None
        else f"Imported trace export: {input_path.as_posix()}"
    )
    gap_status = (
        "The trajectory is still realistic. The ecosystem already has trace collection standards; the gap is semantic packing before storage, replay, and transport."
    )
    reality_status = (
        "This version proves the packer works on built-in fixtures and real export shapes. It does not yet prove wins on a production trace corpus until you point it at one."
    )
    artifact_cards = "\n".join(
        [
            _artifact_card("Trace Sessions", raw_sessions_path.as_posix(), "Raw grouped sessions for this benchmark run."),
            _artifact_card("Semantic Payload", encoded_path.as_posix(), "Readable semantic payload before binary packing."),
            _artifact_card("Summary CSV", summary_path.as_posix(), "Numeric outputs for spreadsheets or further analysis."),
            _artifact_card("Template CSV", templates_path.as_posix(), "Top reusable span structures discovered by the codec."),
            _artifact_card("Stream Window CSV", stream_summary_path.as_posix(), "Per-window transport economics for continuous ingestion."),
            _artifact_card("Retention Tuning CSV", tuning_path.as_posix(), "Candidate active-template caps ranked on longer-lived stream economics."),
            _artifact_card("Pack Manifest", pack_manifest_path.as_posix(), "Replayable bundle metadata with digests and economics."),
            _artifact_card("Packed Payload", pack_payload_path.as_posix(), "Compressed semantic trace pack for machine transport or storage."),
            _artifact_card("Safe Preview", preview_path.as_posix(), "Redacted human-facing preview for review meetings and stakeholder sharing."),
            _artifact_card("Stream Pack Directory", stream_dir.as_posix(), "Incremental window packs for continuous ingestion and replay."),
        ]
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Semantic Trace Pack Report</title>
  <style>
    :root {{
      --bg: #efe6d7;
      --panel: rgba(255, 251, 246, 0.93);
      --panel-soft: rgba(255, 247, 240, 0.82);
      --ink: #182320;
      --muted: #5d655f;
      --line: rgba(24, 35, 32, 0.12);
      --accent: #bf5f2f;
      --accent-soft: rgba(191, 95, 47, 0.12);
      --hero-start: #122b28;
      --hero-end: #35685e;
      --shadow: 0 20px 45px rgba(24, 35, 32, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Aptos", "Segoe UI Variable", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(239, 195, 148, 0.42), transparent 32%),
        radial-gradient(circle at top right, rgba(53, 104, 94, 0.18), transparent 28%),
        linear-gradient(180deg, #f7f0e5 0%, #ece0cf 100%);
    }}
    h1, h2, h3 {{ font-family: "Iowan Old Style", Georgia, serif; margin: 0; letter-spacing: 0.02em; }}
    p {{ margin: 0; }}
    .page {{ width: min(1320px, calc(100% - 24px)); margin: 16px auto 36px; }}
    .hero, .card, .metric-card {{ border: 1px solid var(--line); border-radius: 28px; box-shadow: var(--shadow); }}
    .hero {{
      position: relative;
      overflow: hidden;
      padding: clamp(22px, 3vw, 32px);
      background: linear-gradient(135deg, var(--hero-start), var(--hero-end));
      color: #f9f4ed;
    }}
    .hero::after {{
      content: "";
      position: absolute;
      inset: auto -40px -60px auto;
      width: 240px;
      height: 240px;
      background: radial-gradient(circle, rgba(239, 195, 148, 0.42), transparent 68%);
    }}
    .eyebrow {{
      margin-bottom: 10px;
      color: rgba(249, 244, 237, 0.72);
      text-transform: uppercase;
      letter-spacing: 0.18em;
      font-size: 0.74rem;
    }}
    .hero p {{
      max-width: 900px;
      margin-top: 14px;
      line-height: 1.62;
      color: rgba(249, 244, 237, 0.88);
    }}
    .hero-actions, .nav {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }}
    .hero-actions {{ margin-top: 18px; }}
    .pill {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 9px 12px;
      border-radius: 999px;
      text-decoration: none;
      font-size: 0.84rem;
      font-weight: 700;
      background: rgba(255, 255, 255, 0.08);
      color: #f9f4ed;
      border: 1px solid rgba(255, 255, 255, 0.12);
    }}
    .hero-grid, .grid, .metrics, .artifact-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(min(100%, 220px), 1fr));
      gap: 14px;
    }}
    .hero-grid {{ margin-top: 18px; }}
    .metric-card, .card {{
      background: var(--panel);
      padding: 18px;
      min-width: 0;
    }}
    .soft-card {{ background: var(--panel-soft); }}
    .metric-card span {{
      display: block;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 0.76rem;
    }}
    .metric-card strong {{
      display: block;
      margin-top: 8px;
      font-size: clamp(1.4rem, 2.5vw, 2rem);
      color: var(--ink);
      word-break: break-word;
    }}
    .metric-card p, .card p {{
      margin-top: 10px;
      color: var(--muted);
      line-height: 1.55;
    }}
    .nav {{
      margin-top: 14px;
      padding: 12px;
      border-radius: 22px;
      background: rgba(255, 251, 245, 0.8);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
    }}
    .nav a {{
      padding: 9px 12px;
      border-radius: 999px;
      background: rgba(24, 35, 32, 0.05);
      color: var(--ink);
      text-decoration: none;
      font-size: 0.84rem;
      font-weight: 700;
    }}
    .section {{ margin-top: 20px; }}
    .section-head {{
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 12px;
      align-items: end;
    }}
    .section-head p {{
      max-width: 820px;
      color: var(--muted);
      line-height: 1.55;
    }}
    .callout {{
      background: linear-gradient(180deg, rgba(255, 251, 246, 0.98), rgba(255, 243, 233, 0.96));
    }}
    .callout strong {{
      color: var(--ink);
      display: block;
      margin-bottom: 8px;
      font-size: 1.1rem;
    }}
    .table-shell {{ overflow-x: auto; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 720px; }}
    th, td {{
      text-align: left;
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }}
    th {{
      font-size: 0.76rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }}
    code {{
      font-family: "Consolas", monospace;
      background: rgba(24, 35, 32, 0.06);
      padding: 0.12rem 0.35rem;
      border-radius: 0.3rem;
      word-break: break-word;
    }}
    .artifact-grid {{
      grid-template-columns: repeat(auto-fit, minmax(min(100%, 260px), 1fr));
    }}
    .artifact-card {{
      padding-top: 16px;
      position: relative;
    }}
    .artifact-card::before {{
      content: "";
      position: absolute;
      inset: 0 auto auto 0;
      width: 100%;
      height: 4px;
      background: linear-gradient(90deg, var(--accent), rgba(191, 95, 47, 0.14));
      border-top-left-radius: 28px;
      border-top-right-radius: 28px;
    }}
    .artifact-card strong {{
      display: block;
      font-size: 1.02rem;
      color: var(--ink);
    }}
    .artifact-card code {{
      display: block;
      margin-top: 10px;
    }}
    .report-footer {{
      margin-top: 18px;
      color: var(--muted);
      line-height: 1.55;
      font-size: 0.93rem;
    }}
    @media (max-width: 760px) {{
      .page {{ width: min(100% - 14px, 1320px); margin: 10px auto 24px; }}
      .hero, .card, .metric-card {{ border-radius: 20px; }}
      .metric-card, .card {{ padding: 14px; }}
      table {{ min-width: 560px; }}
    }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
        <p class="eyebrow">Helix | v{escape(VERSION)} | {escape(current_release.code_name)}</p>
      <h1>Semantic Trace Pack Report</h1>
      <p>This version turns the trace benchmark into a product-shaped packaging workflow for OpenTelemetry and OpenInference-style AI traces. The realistic goal is not to replace commodity byte compressors. It is to add a vendor-neutral semantic layer that makes machine-native traces and agent handoffs cheaper to store, move, and replay.</p>
      <div class="hero-actions">
        <span class="pill">Source {escape(str(summary["source_format"]))}</span>
        <span class="pill">Family {escape(str(summary["source_family"]))}</span>
        <span class="pill">Sessions {int(summary["session_count"])}</span>
        <span class="pill">Templates {int(summary["template_count"])}</span>
        <span class="pill">Pack Ratio {float(manifest["pack_ratio_vs_raw"]):.2f}</span>
        <span class="pill">Replay Passes {int(summary["stream_replay_passes"])}</span>
        <span class="pill">Recommended Cap {escape(str(tuning_summary["recommended_max_active_templates"]))}</span>
        <a class="pill" href="version_history.html">Version History</a>
      </div>
      <div class="hero-grid">
        <article class="metric-card soft-card"><span>Trajectory Check</span><strong>Real Product Gap</strong><p>{escape(gap_status)}</p></article>
        <article class="metric-card soft-card"><span>Current Reality</span><strong>Promising, Not Proven</strong><p>{escape(reality_status)}</p></article>
        <article class="metric-card soft-card"><span>Input</span><strong>{escape(source_description)}</strong><p>Trace source used for this benchmark run.</p></article>
        <article class="metric-card soft-card"><span>Gain Vs Raw zlib</span><strong>{int(summary["semantic_plus_zlib_gain_vs_raw_zlib"]):+d}</strong><p>Compressed-byte gain after semantic templating compared with raw JSON + zlib.</p></article>
        <article class="metric-card soft-card"><span>Gain Vs Source Gzip</span><strong>{int(summary["semantic_pack_gain_vs_source_gzip"]):+d}</strong><p>Compressed-byte gain after semantic templating compared with the import source plus deterministic gzip.</p></article>
        <article class="metric-card soft-card"><span>Retention Recommendation</span><strong>{escape(str(tuning_summary["recommended_max_active_templates"]))}</strong><p>{escape(str(tuning_summary["retention_recommendation_reason"]))}</p></article>
      </div>
    </section>

    <nav class="nav">
      <a href="#briefing">Trajectory</a>
      <a href="#economics">Economics</a>
      <a href="#streaming">Streaming</a>
      <a href="#retention">Retention Tuning</a>
      <a href="#compatibility">Compatibility</a>
      <a href="#prototypes">Working Prototypes</a>
      <a href="#artifacts">Artifacts</a>
      <a href="#preview">Safe Preview</a>
      <a href="#templates">Templates</a>
    </nav>

    <section class="section" id="briefing">
      <div class="section-head">
        <div>
          <h2>Why This Version Exists</h2>
          <p>The March 2026 landscape already has trace collection and observability standards. The missing product layer is semantic packing that sits between those standards and long-term storage, replay, or agent-to-agent transport.</p>
        </div>
      </div>
      <div class="grid">
        <article class="card callout">
          <strong>What We Proved</strong>
          <p>The repo can now ingest built-in fixtures, flat exported span rows, and OTLP JSON; encode them into a reusable semantic payload; and emit a compressed pack plus manifest with deterministic replay metadata.</p>
        </article>
        <article class="card callout">
          <strong>What We Have Not Proved Yet</strong>
          <p>This run does not prove product-market fit on its own. We still need real exported workloads from a live AI system to measure pack economics by workflow family and data shape.</p>
        </article>
        <article class="card callout">
          <strong>Release Direction</strong>
          <p>{escape(current_release.summary)}</p>
        </article>
      </div>
    </section>

    <section class="section" id="economics">
      <div class="section-head">
        <div>
          <h2>Compression Economics</h2>
          <p>These are the top-level numbers to watch. The product thesis only holds if semantic packing beats commodity compression on the same machine-native trace workload.</p>
        </div>
      </div>
      <div class="metrics">
        <article class="metric-card"><span>Source Bytes</span><strong>{int(summary["source_bytes"])}</strong><p>Serialized byte size of the imported source file or fixture export.</p></article>
        <article class="metric-card"><span>Source Gzip Bytes</span><strong>{int(summary["source_gzip_bytes"])}</strong><p>Deterministic gzip baseline on the imported source bytes.</p></article>
        <article class="metric-card"><span>Raw JSON Bytes</span><strong>{int(summary["raw_json_bytes"])}</strong><p>Baseline serialized size for the original trace payloads.</p></article>
        <article class="metric-card"><span>Canonical Ratio</span><strong>{float(summary["canonical_ratio_vs_raw"]):.2f}</strong><p>Ratio after removing volatile identifiers and normalizing structure.</p></article>
        <article class="metric-card"><span>Semantic Ratio</span><strong>{float(summary["semantic_ratio_vs_raw"]):.2f}</strong><p>Ratio after template-based semantic encoding before binary packing.</p></article>
        <article class="metric-card"><span>zlib Raw Ratio</span><strong>{float(summary["zlib_raw_ratio_vs_raw"]):.2f}</strong><p>Commodity compression on raw JSON.</p></article>
        <article class="metric-card"><span>gzip Raw Ratio</span><strong>{float(summary["gzip_raw_ratio_vs_raw"]):.2f}</strong><p>Deterministic gzip on normalized raw JSON bytes.</p></article>
        <article class="metric-card"><span>Pack Ratio</span><strong>{float(manifest["pack_ratio_vs_raw"]):.2f}</strong><p>Compressed semantic pack size relative to raw JSON bytes.</p></article>
        <article class="metric-card"><span>Template Reuse</span><strong>{float(summary["template_reuse_rate"]) * 100:.1f}%</strong><p>How much recurring span structure was captured by shared templates.</p></article>
        <article class="metric-card"><span>Pack Bytes</span><strong>{int(manifest["pack_zlib_bytes"])}</strong><p>Final binary pack size for transport or storage.</p></article>
        <article class="metric-card"><span>Lossless Roundtrip</span><strong>{"yes" if bool(manifest["lossless_canonical_roundtrip"]) else "no"}</strong><p>Decoded pack returns the same canonical span structure the codec encoded.</p></article>
        <article class="metric-card"><span>Reproducible</span><strong>{"yes" if int(summary["reproducibility_verified"]) else "no"}</strong><p>Running the same benchmark twice yields the same semantic payload.</p></article>
      </div>
    </section>

    <section class="section" id="streaming">
      <div class="section-head">
        <div>
          <h2>Streaming Window Economics</h2>
          <p>Real observability pipelines do not ship one giant batch forever. They ship windows. This section measures whether semantic packs keep paying off when template memory is carried forward across continuous ingestion windows.</p>
        </div>
      </div>
      <div class="metrics">
        <article class="metric-card"><span>Window Size</span><strong>{int(stream_summary["window_size_sessions"])}</strong><p>Sessions packed together before the stream advances.</p></article>
        <article class="metric-card"><span>Replay Passes</span><strong>{int(summary["stream_replay_passes"])}</strong><p>How many times the same workload family was replayed to stress longer-lived streaming behavior.</p></article>
        <article class="metric-card"><span>Replay Sessions</span><strong>{int(summary["stream_replay_session_count"])}</strong><p>Total sessions fed through the longer-lived stream evaluation.</p></article>
        <article class="metric-card"><span>Window Count</span><strong>{int(stream_summary["window_count"])}</strong><p>How many incremental payloads were generated.</p></article>
        <article class="metric-card"><span>Stream Pack Ratio</span><strong>{float(stream_summary["stream_pack_ratio_vs_raw"]):.2f}</strong><p>Total streaming pack bytes relative to the raw windowed trace bytes.</p></article>
        <article class="metric-card"><span>Gain Vs Windowed Raw zlib</span><strong>{int(stream_summary["stream_pack_gain_vs_windowed_raw_zlib"]):+d}</strong><p>Total byte gain against compressing each raw window independently with zlib.</p></article>
        <article class="metric-card"><span>Cross-Window Reuse</span><strong>{float(stream_summary["cross_window_template_reuse_rate"]) * 100:.1f}%</strong><p>Share of spans that reused templates learned in earlier windows.</p></article>
        <article class="metric-card"><span>New Templates Added</span><strong>{int(stream_summary["stream_new_template_total"])}</strong><p>Total new templates introduced across all windows.</p></article>
        <article class="metric-card"><span>Active Template Cap</span><strong>{escape(str(stream_summary["max_active_templates"]))}</strong><p>Bound on active streaming templates kept for future windows.</p></article>
        <article class="metric-card"><span>Evicted Templates</span><strong>{int(stream_summary["stream_evicted_template_total"])}</strong><p>Templates dropped from active memory to keep the stream bounded.</p></article>
        <article class="metric-card"><span>Reintroduced Templates</span><strong>{int(stream_summary["stream_reintroduced_template_total"])}</strong><p>Templates that had to be learned again after earlier eviction.</p></article>
        <article class="metric-card"><span>Final Active Templates</span><strong>{int(stream_summary["final_active_template_count"])}</strong><p>How many templates remained active at the end of the stream.</p></article>
      </div>
      <div class="card" style="margin-top:14px;">
        <div class="table-shell">
          <table>
            <thead>
              <tr><th>Window</th><th>Sessions</th><th>Spans</th><th>New Templates</th><th>Reintroduced</th><th>Evicted</th><th>Reuse Rate</th><th>Pack Bytes</th><th>Gain</th></tr>
            </thead>
            <tbody>
              {stream_table_rows}
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="section" id="retention">
      <div class="section-head">
        <div>
          <h2>Retention Tuning</h2>
          <p>This section stress-tests bounded active-template caps on a longer-lived replay stream. It exists to answer the product question that matters next: which memory budget actually holds up when traffic keeps coming.</p>
        </div>
      </div>
      <div class="metrics">
        <article class="metric-card"><span>Baseline Cap</span><strong>{escape(str(tuning_summary["baseline_max_active_templates"]))}</strong><p>The active-template cap used for the main stream report above.</p></article>
        <article class="metric-card"><span>Recommended Cap</span><strong>{escape(str(tuning_summary["recommended_max_active_templates"]))}</strong><p>The candidate that maximized stream gain, then broke ties toward lower retained memory and lower churn.</p></article>
        <article class="metric-card"><span>Tuning Uplift</span><strong>{int(tuning_summary["recommended_gain_delta_vs_baseline"]):+d}</strong><p>How much the recommended cap improved streaming gain versus the current baseline cap on the replay stress test.</p></article>
        <article class="metric-card"><span>Recommended Reuse</span><strong>{float(tuning_summary["recommended_cross_window_template_reuse_rate"]) * 100:.1f}%</strong><p>Cross-window template reuse rate at the recommended cap.</p></article>
        <article class="metric-card"><span>Recommended Active Templates</span><strong>{int(tuning_summary["recommended_final_active_template_count"])}</strong><p>How many templates remained active at the end of the tuned replay stream.</p></article>
        <article class="metric-card"><span>Candidate Caps</span><strong>{int(tuning_summary["retention_tuning_candidate_count"])}</strong><p>How many bounded or unbounded template-cap choices were tested deterministically.</p></article>
      </div>
      <div class="card" style="margin-top:14px;">
        <div class="table-shell">
          <table>
            <thead>
              <tr><th>Cap</th><th>Role</th><th>Stream Gain</th><th>Delta Vs Baseline</th><th>Reuse</th><th>Final Active</th><th>Evicted</th><th>Reintroduced</th></tr>
            </thead>
            <tbody>
              {tuning_table_rows}
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="section" id="compatibility">
      <div class="section-head">
        <div>
          <h2>Input Compatibility</h2>
          <p>This version is designed to leap from synthetic traces toward real exported workloads. The loader already supports multiple export shapes and keeps the output deterministic.</p>
        </div>
      </div>
      <div class="grid">
        <article class="card">
          <strong>Built-In Fixtures</strong>
          <p>Deterministic OpenInference-style sessions for RAG support, SQL analytics, and browser research workflows.</p>
        </article>
        <article class="card">
          <strong>Flat Span JSONL</strong>
          <p>Line-delimited span exports grouped by <code>session_id</code>, <code>trace_id</code>, or the closest equivalent attribute.</p>
        </article>
        <article class="card">
          <strong>OTLP JSON</strong>
          <p>OpenTelemetry <code>resourceSpans</code> payloads, including resource attributes, span attributes, events, and scope metadata.</p>
        </article>
        <article class="card">
          <strong>Langfuse Trace Export</strong>
          <p>Official Langfuse trace exports with top-level <code>trace</code> and <code>observations</code> objects, normalized into replayable sessions without changing the canonical pack contract.</p>
        </article>
      </div>
      <div class="grid" style="margin-top:14px;">
        {workflow_cards}
      </div>
    </section>

    <section class="section" id="prototypes">
      <div class="section-head">
        <div>
          <h2>Working Prototypes</h2>
          <p>This page is meant to help a board or design partner understand how the product would be piloted. These are the three concrete deployment shapes the current codebase can already demonstrate locally.</p>
        </div>
      </div>
      <div class="artifact-grid">
        {prototype_cards}
      </div>
    </section>

    <section class="section" id="artifacts">
      <div class="section-head">
        <div>
          <h2>Pack Outputs</h2>
          <p>This page is now part dashboard, part documentation, and part landing page. These artifacts are the working product outputs of the current version.</p>
        </div>
      </div>
      <div class="artifact-grid">
        {artifact_cards}
        {stream_artifact_cards}
      </div>
    </section>

    <section class="section" id="preview">
      <div class="section-head">
        <div>
          <h2>Safe Preview</h2>
          <p>The report now includes a privacy-safer preview surface for review meetings. It redacts sensitive text-like fields in the preview artifact without changing the packed machine payload.</p>
        </div>
      </div>
      <div class="metrics">
        <article class="metric-card"><span>Preview Policy</span><strong>{escape(str(preview["policy_name"]))}</strong><p>Explicit policy file that controls preview redaction and clipping.</p></article>
        <article class="metric-card"><span>Bundle Payload Mode</span><strong>{escape(str(preview["bundle_payload_mode"]))}</strong><p>The current safety posture for the canonical pack payload.</p></article>
        <article class="metric-card"><span>Preview Sessions</span><strong>{int(preview["preview_session_count"])}</strong><p>How many sessions were included in the human-facing preview.</p></article>
        <article class="metric-card"><span>Preview Spans</span><strong>{int(preview["preview_span_count"])}</strong><p>How many spans were shown after clipping the preview for readability.</p></article>
        <article class="metric-card"><span>Redacted Fields</span><strong>{int(preview["redacted_field_count"])}</strong><p>Count of preview fields replaced with deterministic redaction summaries.</p></article>
      </div>
      <div class="card">
        <div class="table-shell">
          <table>
            <thead>
              <tr><th>Workflow</th><th>Span</th><th>Kind</th><th>Preview Attributes</th></tr>
            </thead>
            <tbody>
              {preview_table_rows}
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="section" id="templates">
      <div class="section-head">
        <div>
          <h2>Top Templates</h2>
          <p>These are the reusable span skeletons the codec discovered. If this product succeeds, this template layer becomes the persistent semantic substrate for trace storage and handoff replay.</p>
        </div>
      </div>
      <div class="card">
        <div class="table-shell">
          <table>
            <thead>
              <tr><th>ID</th><th>Name</th><th>Kind</th><th>Uses</th><th>Attribute Keys</th></tr>
            </thead>
            <tbody>
              {template_table_rows}
            </tbody>
          </table>
        </div>
      </div>
      <p class="report-footer">Grounding: this product direction is aligned to OpenTelemetry traces and OpenInference semantic conventions. The product is the semantic pack layer above those standards, not a replacement for them.</p>
    </section>
  </main>
</body>
</html>
"""


def _artifact_card(title: str, path: str, description: str) -> str:
    return (
        '<article class="card artifact-card">'
        f"<strong>{escape(title)}</strong>"
        f"<p>{escape(description)}</p>"
        f"<code>{escape(path)}</code>"
        "</article>"
    )


def _preview_attribute_summary(attributes: Mapping[str, object]) -> str:
    parts: list[str] = []
    for key, value in list(attributes.items())[:3]:
        parts.append(f"{key}={value}")
    return ", ".join(parts)


def _pretty_json(payload: object) -> str:
    return json.dumps(payload, indent=2, sort_keys=True)


def _build_trace_version_history_html() -> str:
    cards = "\n".join(
        (
            '<article class="card">'
            f"<small>{escape(record.released_on)}</small>"
            f"<h3>v{escape(record.version)} | {escape(record.code_name)}</h3>"
            f"<p><strong>Focus:</strong> {escape(record.focus)}</p>"
            f"<p>{escape(record.summary)}</p>"
            f"<p><strong>Next:</strong> {escape(record.next_step)}</p>"
            "</article>"
        )
        for record in reversed(VERSION_HISTORY)
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Helix - Version History</title>
  <style>
    :root {{ --bg: #f7f0e5; --panel: rgba(255, 251, 245, 0.92); --ink: #182320; --muted: #5d655f; --line: rgba(24, 35, 32, 0.12); }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Aptos", "Segoe UI", sans-serif; color: var(--ink); background: linear-gradient(180deg, #f7f0e5 0%, #ece0cf 100%); }}
    .page {{ width: min(1120px, calc(100% - 24px)); margin: 18px auto 32px; }}
    .hero, .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 24px; box-shadow: 0 18px 40px rgba(24, 35, 32, 0.06); }}
    .hero {{ padding: 24px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 14px; margin-top: 18px; }}
    .card {{ padding: 18px; }}
    h1, h2, h3 {{ font-family: "Iowan Old Style", Georgia, serif; margin: 0; }}
    p, small {{ color: var(--muted); line-height: 1.55; }}
    a {{ color: #bf5f2f; text-decoration: none; font-weight: 700; }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <small>Helix</small>
      <h1>Version History</h1>
      <p>This page tracks the versions that moved the project from synthetic protocol experiments toward a real trace compression product.</p>
      <p><a href="latest_trace_benchmark.html">Return To Latest Trace Report</a></p>
    </section>
    <section class="grid">{cards}</section>
  </main>
</body>
</html>
"""
