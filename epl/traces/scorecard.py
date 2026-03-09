from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from html import escape
from pathlib import Path

from epl.analysis.dashboard import refresh_latest_dashboard
from epl.logging_utils import ensure_dirs, write_csv
from epl.traces.codec import benchmark_trace_codec
from epl.traces.corpus import discover_trace_sources
from epl.traces.fixtures import detect_trace_input_format, load_trace_sessions
from epl.traces.package import build_trace_pack
from epl.traces.schema import TraceSession
from epl.traces.tuning import tune_stream_retention
from epl.version import VERSION
from epl.version_history import current_version_record


def run_trace_scorecard(
    *,
    output_dir: Path = Path("outputs"),
    input_path: Path | None = None,
    window_size: int = 6,
    max_active_templates: int | None = 8,
    stream_replay_passes: int = 4,
    retention_candidates: Sequence[int | None] | None = None,
) -> dict[str, object]:
    output_dir = Path(output_dir)
    scorecard_dir = output_dir / "scorecard"
    dashboard_dir = output_dir / "dashboard"
    ensure_dirs(scorecard_dir, dashboard_dir)

    sources = discover_trace_sources(input_path)
    if not sources:
        raise ValueError("no supported trace sources found")

    workflow_sessions: dict[str, list[TraceSession]] = defaultdict(list)
    for source in sources:
        sessions = load_trace_sessions(source, input_format=detect_trace_input_format(source))
        for session in sessions:
            workflow_sessions[session.workflow].append(session)

    workflow_rows: list[dict[str, object]] = []
    for workflow in sorted(workflow_sessions):
        sessions = workflow_sessions[workflow]
        benchmark = benchmark_trace_codec(sessions)
        pack = build_trace_pack(
            benchmark_result=benchmark,
            run_id=f"scorecard-{workflow}",
            source_label=workflow,
            source_format="workflow_group",
        )
        tuning = tune_stream_retention(
            sessions,
            window_size=min(window_size, max(len(sessions), 1)),
            replay_passes=stream_replay_passes,
            candidate_caps=retention_candidates,
            baseline_cap=max_active_templates,
        )
        workflow_rows.append(
            {
                "workflow": workflow,
                "session_count": int(benchmark["summary"]["session_count"]),
                "span_count": int(benchmark["summary"]["span_count"]),
                "template_count": int(benchmark["summary"]["template_count"]),
                "zlib_raw_bytes": int(benchmark["summary"]["zlib_raw_bytes"]),
                "pack_zlib_bytes": int(pack.manifest["pack_zlib_bytes"]),
                "semantic_pack_gain_vs_raw_zlib": int(benchmark["summary"]["semantic_plus_zlib_gain_vs_raw_zlib"]),
                "recommended_max_active_templates": str(tuning["summary"]["recommended_max_active_templates"]),
                "recommended_gain_delta_vs_baseline": int(tuning["summary"]["recommended_gain_delta_vs_baseline"]),
                "recommended_cross_window_template_reuse_rate": float(tuning["summary"]["recommended_cross_window_template_reuse_rate"]),
                "pilot_ready": int(
                    benchmark["summary"]["semantic_plus_zlib_gain_vs_raw_zlib"] > 0
                    and int(tuning["summary"]["recommended_gain_delta_vs_baseline"]) >= 0
                    and int(benchmark["summary"]["session_count"]) >= 1
                ),
            }
        )

    summary = _build_scorecard_summary(workflow_rows)
    run_id = _build_run_id()
    summary_path = scorecard_dir / f"{run_id}_trace_scorecard_summary.csv"
    workflows_path = scorecard_dir / f"{run_id}_trace_scorecard_workflows.csv"
    report_path = dashboard_dir / f"{run_id}_trace_scorecard.html"
    latest_report_path = dashboard_dir / "latest_trace_scorecard.html"
    write_csv(summary_path, [summary])
    write_csv(workflows_path, workflow_rows)
    html = _build_scorecard_report_html(summary=summary, rows=workflow_rows)
    report_path.write_text(html, encoding="utf-8")
    latest_report_path.write_text(html, encoding="utf-8")
    refresh_latest_dashboard(output_dir)

    return {
        "version": VERSION,
        "run_id": run_id,
        **summary,
        "summary_path": str(summary_path),
        "workflows_path": str(workflows_path),
        "report_path": str(report_path),
        "latest_report_path": str(latest_report_path),
    }


def _build_scorecard_summary(rows: Sequence[dict[str, object]]) -> dict[str, object]:
    pilot_ready = [row for row in rows if int(row["pilot_ready"]) == 1]
    strongest = max(rows, key=lambda row: int(row["semantic_pack_gain_vs_raw_zlib"]), default=None)
    weakest = min(rows, key=lambda row: int(row["semantic_pack_gain_vs_raw_zlib"]), default=None)
    return {
        "workflow_count": len(rows),
        "pilot_ready_workflow_count": len(pilot_ready),
        "recommended_max_active_templates": _recommend_scorecard_cap(rows),
        "strongest_workflow": strongest["workflow"] if strongest is not None else "none",
        "strongest_workflow_gain": int(strongest["semantic_pack_gain_vs_raw_zlib"]) if strongest is not None else 0,
        "weakest_workflow": weakest["workflow"] if weakest is not None else "none",
        "weakest_workflow_gain": int(weakest["semantic_pack_gain_vs_raw_zlib"]) if weakest is not None else 0,
    }


def _build_scorecard_report_html(*, summary: dict[str, object], rows: Sequence[dict[str, object]]) -> str:
    current_release = current_version_record()
    row_html = "\n".join(
        "<tr>"
        f"<td><code>{escape(str(row['workflow']))}</code></td>"
        f"<td>{int(row['session_count'])}</td>"
        f"<td>{int(row['span_count'])}</td>"
        f"<td>{int(row['semantic_pack_gain_vs_raw_zlib']):+d}</td>"
        f"<td><code>{escape(str(row['recommended_max_active_templates']))}</code></td>"
        f"<td>{int(row['recommended_gain_delta_vs_baseline']):+d}</td>"
        f"<td>{float(row['recommended_cross_window_template_reuse_rate']) * 100:.1f}%</td>"
        f"<td>{'yes' if int(row['pilot_ready']) else 'not yet'}</td>"
        "</tr>"
        for row in rows
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="10">
  <title>Trace Workflow Scorecard</title>
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
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <p>Helix | v{escape(VERSION)} | {escape(current_release.code_name)}</p>
      <h1>Trace Workflow Scorecard</h1>
      <p>This scorecard asks the question that matters before a pilot: which workflow families are actually strong enough to be good real-world candidates, and which ones still need work.</p>
    </section>
    <section class="section">
      <div class="grid">
        <article class="card metric"><span>Workflows</span><strong>{int(summary["workflow_count"])}</strong></article>
        <article class="card metric"><span>Pilot-Ready Workflows</span><strong>{int(summary["pilot_ready_workflow_count"])}</strong></article>
        <article class="card metric"><span>Recommended Active Cap</span><strong>{escape(str(summary["recommended_max_active_templates"]))}</strong></article>
        <article class="card metric"><span>Strongest Workflow</span><strong>{escape(str(summary["strongest_workflow"]))}</strong></article>
        <article class="card metric"><span>Weakest Workflow</span><strong>{escape(str(summary["weakest_workflow"]))}</strong></article>
      </div>
    </section>
    <section class="section">
      <h2>Workflow Detail</h2>
      <p>Use this table to see where semantic packing is strongest and where the product is still too early for pilot claims.</p>
      <div class="card">
        <div class="table-shell">
          <table>
            <thead>
              <tr><th>Workflow</th><th>Sessions</th><th>Spans</th><th>Gain Vs Raw zlib</th><th>Recommended Cap</th><th>Tuning Uplift</th><th>Reuse</th><th>Pilot Ready</th></tr>
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


def _recommend_scorecard_cap(rows: Sequence[dict[str, object]]) -> str:
    if not rows:
        return "unknown"
    scores: dict[str, tuple[int, int]] = {}
    for row in rows:
        cap = str(row["recommended_max_active_templates"])
        count, gain = scores.get(cap, (0, 0))
        scores[cap] = (count + 1, gain + int(row["recommended_gain_delta_vs_baseline"]))
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


def _build_run_id() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
