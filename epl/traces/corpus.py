from __future__ import annotations

import csv
from collections.abc import Sequence
from html import escape
from pathlib import Path

from epl.analysis.dashboard import refresh_latest_dashboard
from epl.logging_utils import ensure_dirs, write_csv
from epl.traces.benchmark import run_trace_benchmark
from epl.version import VERSION
from epl.version_history import current_version_record


SUPPORTED_TRACE_SUFFIXES = {".json", ".jsonl"}


def run_trace_corpus(
    *,
    output_dir: Path = Path("outputs"),
    input_path: Path | None = None,
    window_size: int = 6,
    max_active_templates: int | None = 8,
    stream_replay_passes: int = 4,
    retention_candidates: Sequence[int | None] | None = None,
) -> dict[str, object]:
    output_dir = Path(output_dir)
    corpus_dir = output_dir / "corpus"
    dashboard_dir = output_dir / "dashboard"
    ensure_dirs(corpus_dir, dashboard_dir)

    sources = discover_trace_sources(input_path)
    if not sources:
        raise ValueError("no supported trace sources found")

    aggregate_rows: list[dict[str, object]] = []
    source_results: list[dict[str, object]] = []
    for index, source in enumerate(sources, start=1):
        slug = f"{index:02d}_{_slugify(source.stem)}"
        source_output_dir = corpus_dir / slug
        result = run_trace_benchmark(
            output_dir=source_output_dir,
            input_path=source,
            window_size=window_size,
            max_active_templates=max_active_templates,
            stream_replay_passes=stream_replay_passes,
            retention_candidates=retention_candidates,
        )
        report_path = Path(result["report_path"]).relative_to(output_dir).as_posix()
        row = {
            "source_name": source.name,
            "source_path": source.as_posix(),
            "source_format": result["source_format"],
            "source_family": result["source_family"],
            "session_count": result["session_count"],
            "span_count": result["span_count"],
            "template_count": result["template_count"],
            "source_bytes": result["source_bytes"],
            "source_gzip_bytes": result["source_gzip_bytes"],
            "pack_ratio_vs_raw": result["pack_ratio_vs_raw"],
            "semantic_plus_zlib_gain_vs_raw_zlib": result["semantic_plus_zlib_gain_vs_raw_zlib"],
            "semantic_pack_gain_vs_source_gzip": result["semantic_pack_gain_vs_source_gzip"],
            "stream_pack_gain_vs_windowed_raw_zlib": result["stream_pack_gain_vs_windowed_raw_zlib"],
            "cross_window_template_reuse_rate": result["cross_window_template_reuse_rate"],
            "canonical_roundtrip_verified": result["canonical_roundtrip_verified"],
            "reproducibility_verified": result["reproducibility_verified"],
            "max_active_templates": result["max_active_templates"],
            "stream_replay_passes": result["stream_replay_passes"],
            "recommended_max_active_templates": result["recommended_max_active_templates"],
            "recommended_gain_delta_vs_baseline": result["recommended_gain_delta_vs_baseline"],
            "report_path": report_path,
        }
        aggregate_rows.append(row)
        source_results.append(result)

    run_id = source_results[-1]["run_id"]
    summary = _build_corpus_summary(aggregate_rows, len(sources))
    summary_path = corpus_dir / f"{run_id}_trace_corpus_summary.csv"
    sources_path = corpus_dir / f"{run_id}_trace_corpus_sources.csv"
    report_path = dashboard_dir / f"{run_id}_trace_corpus.html"
    latest_report_path = dashboard_dir / "latest_trace_corpus.html"
    write_csv(summary_path, [summary])
    write_csv(sources_path, aggregate_rows)
    html = _build_corpus_report_html(summary=summary, rows=aggregate_rows)
    report_path.write_text(html, encoding="utf-8")
    latest_report_path.write_text(html, encoding="utf-8")
    refresh_latest_dashboard(output_dir)

    return {
        "version": VERSION,
        "run_id": run_id,
        **summary,
        "summary_path": str(summary_path),
        "sources_path": str(sources_path),
        "report_path": str(report_path),
        "latest_report_path": str(latest_report_path),
    }


def discover_trace_sources(input_path: Path | None) -> list[Path]:
    if input_path is None:
        input_path = Path("data/opensource/traces")

    input_path = Path(input_path)
    if input_path.is_file():
        return [input_path]
    if input_path.is_dir():
        return sorted(
            path
            for path in input_path.rglob("*")
            if path.is_file() and path.suffix.lower() in SUPPORTED_TRACE_SUFFIXES
        )
    raise ValueError(f"trace source path does not exist: {input_path}")


def _build_corpus_summary(rows: Sequence[dict[str, object]], source_count: int) -> dict[str, object]:
    session_total = sum(int(row["session_count"]) for row in rows)
    span_total = sum(int(row["span_count"]) for row in rows)
    stream_gain_total = sum(int(row["stream_pack_gain_vs_windowed_raw_zlib"]) for row in rows)
    zlib_gain_total = sum(int(row["semantic_plus_zlib_gain_vs_raw_zlib"]) for row in rows)
    source_gzip_total = sum(int(row["source_gzip_bytes"]) for row in rows)
    semantic_gain_vs_source_gzip_total = sum(int(row["semantic_pack_gain_vs_source_gzip"]) for row in rows)
    avg_pack_ratio = sum(float(row["pack_ratio_vs_raw"]) for row in rows) / max(len(rows), 1)
    avg_cross_window_reuse = sum(float(row["cross_window_template_reuse_rate"]) for row in rows) / max(len(rows), 1)
    avg_recommended_gain_delta = sum(int(row["recommended_gain_delta_vs_baseline"]) for row in rows) / max(len(rows), 1)
    family_count = len({str(row["source_family"]) for row in rows})
    canonical_verified = all(int(row["canonical_roundtrip_verified"]) == 1 for row in rows)
    reproducibility_verified = all(int(row["reproducibility_verified"]) == 1 for row in rows)
    positive_source_gzip_gain_count = sum(int(row["semantic_pack_gain_vs_source_gzip"]) > 0 for row in rows)
    negative_source_gzip_gain_count = sum(int(row["semantic_pack_gain_vs_source_gzip"]) <= 0 for row in rows)
    recommended_cap = _recommend_corpus_cap(rows)
    return {
        "source_count": source_count,
        "source_family_count": family_count,
        "session_total": session_total,
        "span_total": span_total,
        "avg_pack_ratio_vs_raw": round(avg_pack_ratio, 4),
        "avg_cross_window_template_reuse_rate": round(avg_cross_window_reuse, 4),
        "stream_gain_total_vs_windowed_raw_zlib": stream_gain_total,
        "semantic_gain_total_vs_raw_zlib": zlib_gain_total,
        "source_gzip_total_bytes": source_gzip_total,
        "semantic_gain_total_vs_source_gzip": semantic_gain_vs_source_gzip_total,
        "positive_source_gzip_gain_count": positive_source_gzip_gain_count,
        "negative_source_gzip_gain_count": negative_source_gzip_gain_count,
        "canonical_roundtrip_verified": int(canonical_verified),
        "reproducibility_verified": int(reproducibility_verified),
        "max_active_templates": rows[0].get("max_active_templates") if rows else "unknown",
        "stream_replay_passes": rows[0].get("stream_replay_passes") if rows else 0,
        "recommended_max_active_templates": recommended_cap,
        "avg_recommended_gain_delta_vs_baseline": round(avg_recommended_gain_delta, 2),
    }


def _build_corpus_report_html(*, summary: dict[str, object], rows: Sequence[dict[str, object]]) -> str:
    current_release = current_version_record()
    row_html = "\n".join(
        "<tr>"
        f"<td><code>{escape(str(row['source_name']))}</code></td>"
        f"<td><code>{escape(str(row['source_format']))}</code></td>"
        f"<td><code>{escape(str(row['source_family']))}</code></td>"
        f"<td>{int(row['session_count'])}</td>"
        f"<td>{int(row['span_count'])}</td>"
        f"<td>{int(row['template_count'])}</td>"
        f"<td>{float(row['pack_ratio_vs_raw']):.2f}</td>"
        f"<td>{int(row['semantic_plus_zlib_gain_vs_raw_zlib']):+d}</td>"
        f"<td>{int(row['semantic_pack_gain_vs_source_gzip']):+d}</td>"
        f"<td>{int(row['stream_pack_gain_vs_windowed_raw_zlib']):+d}</td>"
        f"<td>{float(row['cross_window_template_reuse_rate']) * 100:.1f}%</td>"
        f"<td>{'yes' if int(row['canonical_roundtrip_verified']) else 'no'}</td>"
        f"<td>{'yes' if int(row['reproducibility_verified']) else 'no'}</td>"
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
  <title>Trace Corpus Report</title>
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
    body {{ margin: 0; font-family: "Aptos", "Segoe UI", sans-serif; color: var(--ink); background: linear-gradient(180deg, #f7f0e5 0%, #ece0cf 100%); }}
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
    code {{ font-family: "Consolas", monospace; background: rgba(24, 35, 32, 0.06); padding: 0.12rem 0.35rem; border-radius: 0.3rem; }}
    a {{ color: var(--accent); text-decoration: none; font-weight: 700; }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <p>Helix | v{escape(VERSION)} | {escape(current_release.code_name)}</p>
      <h1>Trace Corpus Report</h1>
      <p>This report benchmarks a whole export set instead of a single handpicked file. That is the minimum honest surface for release work: repeated wins across sources, formats, and ingestion windows.</p>
    </section>

    <section class="section">
      <div class="grid">
        <article class="card metric"><span>Sources</span><strong>{int(summary["source_count"])}</strong></article>
        <article class="card metric"><span>Families</span><strong>{int(summary["source_family_count"])}</strong></article>
        <article class="card metric"><span>Sessions</span><strong>{int(summary["session_total"])}</strong></article>
        <article class="card metric"><span>Spans</span><strong>{int(summary["span_total"])}</strong></article>
        <article class="card metric"><span>Avg Pack Ratio</span><strong>{float(summary["avg_pack_ratio_vs_raw"]):.2f}</strong></article>
        <article class="card metric"><span>Avg Cross-Window Reuse</span><strong>{float(summary["avg_cross_window_template_reuse_rate"]) * 100:.1f}%</strong></article>
        <article class="card metric"><span>Total Stream Gain</span><strong>{int(summary["stream_gain_total_vs_windowed_raw_zlib"]):+d}</strong></article>
        <article class="card metric"><span>Pack Gain Vs Gzip</span><strong>{int(summary["semantic_gain_total_vs_source_gzip"]):+d}</strong></article>
        <article class="card metric"><span>Replay Verified</span><strong>{"yes" if int(summary["canonical_roundtrip_verified"]) else "no"}</strong></article>
        <article class="card metric"><span>Reproducible</span><strong>{"yes" if int(summary["reproducibility_verified"]) else "no"}</strong></article>
        <article class="card metric"><span>Positive Sources</span><strong>{int(summary["positive_source_gzip_gain_count"])}</strong></article>
        <article class="card metric"><span>Replay Passes</span><strong>{int(summary["stream_replay_passes"])}</strong></article>
        <article class="card metric"><span>Recommended Active Cap</span><strong>{escape(str(summary["recommended_max_active_templates"]))}</strong></article>
        <article class="card metric"><span>Avg Tuning Uplift</span><strong>{float(summary["avg_recommended_gain_delta_vs_baseline"]):+.2f}</strong></article>
      </div>
    </section>

    <section class="section">
      <h2>Corpus Detail</h2>
      <p>Use this table to see whether the product thesis is holding up source by source. The important release question is whether gains, replay integrity, and retention recommendations repeat, and where the packer still loses.</p>
      <div class="card">
        <div class="table-shell">
          <table>
            <thead>
              <tr><th>Source</th><th>Format</th><th>Family</th><th>Sessions</th><th>Spans</th><th>Templates</th><th>Pack Ratio</th><th>Batch Gain</th><th>Pack Vs Gzip</th><th>Stream Gain</th><th>Reuse</th><th>Replay</th><th>Repeatable</th><th>Recommended Cap</th><th>Tuning Uplift</th><th>Report</th></tr>
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


def _recommend_corpus_cap(rows: Sequence[dict[str, object]]) -> str:
    if not rows:
        return "unknown"
    scores: dict[str, tuple[int, int]] = {}
    for row in rows:
        cap = str(row["recommended_max_active_templates"])
        count, uplift_total = scores.get(cap, (0, 0))
        scores[cap] = (count + 1, uplift_total + int(row["recommended_gain_delta_vs_baseline"]))
    ranked_caps = sorted(
        scores.items(),
        key=lambda item: (
            item[1][0],
            item[1][1],
            -(10**9 if item[0] == "unbounded" else int(item[0])),
        ),
        reverse=True,
    )
    return ranked_caps[0][0]


def _slugify(value: str) -> str:
    cleaned = []
    for char in value.lower():
        cleaned.append(char if char.isalnum() else "_")
    return "".join(cleaned).strip("_") or "source"
