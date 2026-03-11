import csv
import json
import os
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from html import escape
from pathlib import Path

from epl.analysis.board_agent import (
    build_board_letter,
    build_board_letter_html,
    build_board_letter_markdown,
)
from epl.analysis.design_agent import build_design_brief
from epl.beta_gate import evaluate_beta_gate
from epl.version_history import VERSION_HISTORY, current_version_record


def generate_dashboard(
    *,
    episode_rows: Sequence[Mapping[str, object]],
    summary_row: Mapping[str, object],
    plot_paths: Sequence[Path],
    output_dir: Path,
    run_id: str,
    mode: str,
    budget: int,
    seed: int,
    task_length: int,
    max_chunk_size: int,
    promote_threshold: int,
    max_vocabulary_size: int,
    memory_entry_cost: int,
    lexicon: Mapping[str, str],
    memory_path: str | None,
    policy_rows: Sequence[Mapping[str, object]],
    policy_dataset_path: str,
    policy_history_path: str | None,
    policy_model_path: str | None,
) -> dict[str, Path]:
    if not episode_rows:
        raise ValueError("episode_rows must not be empty")

    output_dir.mkdir(parents=True, exist_ok=True)

    dashboard_path = output_dir / f"{run_id}_dashboard.html"
    latest_dashboard_path = output_dir / "latest_dashboard.html"
    version_history_path = output_dir / "version_history.html"
    html = _build_dashboard_html(
        episode_rows=episode_rows,
        summary_row=summary_row,
        plot_paths=plot_paths,
        dashboard_dir=output_dir,
        run_id=run_id,
        mode=mode,
        budget=budget,
        seed=seed,
        task_length=task_length,
        max_chunk_size=max_chunk_size,
        promote_threshold=promote_threshold,
        max_vocabulary_size=max_vocabulary_size,
        memory_entry_cost=memory_entry_cost,
        lexicon=lexicon,
        memory_path=memory_path,
        policy_rows=policy_rows,
        policy_dataset_path=policy_dataset_path,
        policy_history_path=policy_history_path,
        policy_model_path=policy_model_path,
    )
    history_html = _build_version_history_html(output_dir)
    dashboard_path.write_text(html, encoding="utf-8")
    version_history_path.write_text(history_html, encoding="utf-8")
    refresh_latest_dashboard(output_dir.parent, latest_legacy_dashboard_name=dashboard_path.name)

    return {
        "dashboard_path": dashboard_path,
        "latest_dashboard_path": latest_dashboard_path,
        "version_history_path": version_history_path,
    }


def refresh_latest_dashboard(output_root: Path, *, latest_legacy_dashboard_name: str | None = None) -> Path:
    dashboard_dir = output_root / "dashboard"
    dashboard_dir.mkdir(parents=True, exist_ok=True)
    latest_dashboard_path = dashboard_dir / "latest_dashboard.html"
    html, board_letter_html, board_letter_markdown = _build_program_landing_html(
        output_root=output_root,
        latest_legacy_dashboard_name=latest_legacy_dashboard_name,
    )
    latest_dashboard_path.write_text(html, encoding="utf-8")
    (dashboard_dir / "latest_board_letter.html").write_text(board_letter_html, encoding="utf-8")
    (dashboard_dir / "latest_board_letter.md").write_text(board_letter_markdown, encoding="utf-8")
    try:
        from epl.site import build_public_site
        build_public_site(output_root=output_root)
    except ImportError:
        pass  # epl.site is only available in the enterprise edition
    return latest_dashboard_path


def _build_dashboard_html(
    *,
    episode_rows: Sequence[Mapping[str, object]],
    summary_row: Mapping[str, object],
    plot_paths: Sequence[Path],
    dashboard_dir: Path,
    run_id: str,
    mode: str,
    budget: int,
    seed: int,
    task_length: int,
    max_chunk_size: int,
    promote_threshold: int,
    max_vocabulary_size: int,
    memory_entry_cost: int,
    lexicon: Mapping[str, str],
    memory_path: str | None,
    policy_rows: Sequence[Mapping[str, object]],
    policy_dataset_path: str,
    policy_history_path: str | None,
    policy_model_path: str | None,
) -> str:
    current_release = current_version_record()
    design = build_design_brief(dict(summary_row), current_release)
    task_rows = _build_task_breakdown(episode_rows)
    message_rows = _build_message_breakdown(episode_rows)
    recent_rows = list(episode_rows[-12:])
    failed_rows = [row for row in episode_rows if not bool(row["success"])]
    plot_cards = "\n".join(_render_plot_card(path, dashboard_dir) for path in plot_paths)
    task_breakdown = "\n".join(_render_task_row(row) for row in task_rows)
    message_breakdown = "\n".join(_render_message_row(row) for row in message_rows[:10])
    recent_episodes = "\n".join(_render_episode_row(row) for row in reversed(recent_rows))
    failed_episodes = "\n".join(_render_episode_row(row) for row in reversed(failed_rows[-8:]))
    lexicon_rows = "\n".join(_render_lexicon_row(sequence, token) for sequence, token in sorted(lexicon.items()))
    top_policy_rows = list(policy_rows[:12])
    policy_table_rows = "\n".join(_render_policy_row(row) for row in top_policy_rows)
    summary_guide_cards = "\n".join(_render_summary_guide_card(*spec) for spec in _summary_guide_specs())
    briefing_cards = "\n".join(_render_brief_card(card) for card in design.briefing_cards)
    timeline_cards = "\n".join(_render_timeline_card(record, current_release.version) for record in reversed(VERSION_HISTORY))
    artifact_rows = "\n".join(
        [
            _render_artifact_row("Latest Dashboard", "latest_dashboard.html"),
            _render_artifact_row("Version History", "version_history.html"),
            _render_artifact_row("Chunk Policy Dataset", policy_dataset_path.replace(chr(92), "/")),
            _render_artifact_row(
                "Policy History",
                policy_history_path.replace(chr(92), "/") if policy_history_path is not None else "not available for this mode",
            ),
            _render_artifact_row(
                "Policy Model",
                policy_model_path.replace(chr(92), "/") if policy_model_path is not None else "not available for this mode",
            ),
            _render_artifact_row(
                "Persistent Memory",
                memory_path.replace(chr(92), "/") if memory_path is not None else "disabled for this mode",
            ),
        ]
    )
    policy_cards = _build_policy_cards(summary_row)
    overall_net_ratio = float(summary_row["net_compression_ratio"])
    overall_net_savings = int(summary_row["net_token_savings"])
    eval_episodes = int(summary_row.get("eval_episodes", 0))

    if not failed_episodes:
        failed_episodes = '<tr><td colspan="9" class="empty-state">No failed episodes in this run.</td></tr>'
    if not lexicon_rows:
        lexicon_rows = '<tr><td colspan="2" class="empty-state">No learned chunks currently retained.</td></tr>'
    if not policy_table_rows:
        policy_table_rows = '<tr><td colspan="7" class="empty-state">No chunk-candidate rows were generated for this run.</td></tr>'

    summary_cards = "\n".join(
        [
            _render_metric_card("Success Rate", _format_percent(float(summary_row["success_rate"])), "How often the receiver reconstructed the exact expected output across train and eval."),
            _render_metric_card("Chunk Hit Rate", _format_percent(float(summary_row["dictionary_hit_rate"])), "How often at least one learned chunk token was used in the message."),
            _render_metric_card("Avg Message Length", _format_float(float(summary_row["average_message_length"])), "Average number of tokens the sender had to transmit per episode."),
            _render_metric_card("Net Compression Ratio", _format_float(overall_net_ratio), "Full-system cost across the whole run. Below 1.00 means the codec beat literal transmission after memory cost."),
            _render_metric_card("Net Token Savings", _format_signed_int(overall_net_savings), "How many total tokens the whole system saved after memory cost was charged."),
            _render_metric_card("Memory Cost", str(int(summary_row["retained_memory_cost"])), "The retained dictionary cost charged against the run using the configured per-entry penalty."),
            _render_metric_card("Persisted Chunks", str(int(summary_row["persisted_memory_entries"])), "How many chunk entries were kept and saved for future runs."),
            _render_metric_card("Loaded Chunks", str(int(summary_row["loaded_memory_entries"])), "How many chunk entries were available at the start of this run."),
            _render_metric_card("Policy Rows", str(int(summary_row.get("policy_candidate_rows", 0))), "How many chunk-candidate examples were exported for future learned memory-policy work."),
            _render_metric_card("Intervention Rows", str(int(summary_row.get("policy_intervention_rows", 0))), "How many chunk candidates actually differed between the learned branch and the rule baseline."),
            _render_metric_card("Stability Proxy", _format_percent(float(summary_row["protocol_stability_proxy"])), "How consistently the same output led to the same message pattern."),
        ]
    )

    eval_cards = _build_eval_cards(summary_row, eval_episodes)
    signal_cards = "\n".join(_render_signal_card(signal) for signal in design.hero_signals)


    return f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <meta http-equiv=\"refresh\" content=\"10\">
  <title>Helix Dashboard - {escape(run_id)}</title>
  <style>
    :root {{
      --bg: #f4efe6;
      --bg-panel: rgba(255, 250, 242, 0.92);
      --bg-strong: #16302b;
      --ink: #1d2521;
      --muted: #5c655f;
      --line: rgba(22, 48, 43, 0.14);
      --accent: {design.accent};
      --accent-soft: {design.accent_soft};
      --success: #2c6a4a;
      --failure: #8f3b2e;
      --shadow: 0 18px 40px rgba(29, 37, 33, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Aptos", "Segoe UI Variable", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(239, 201, 167, 0.55), transparent 32%),
        radial-gradient(circle at top right, rgba(47, 111, 98, 0.18), transparent 28%),
        linear-gradient(180deg, #f7f1e7 0%, #efe5d5 100%);
    }}
    .page {{ width: min(1320px, calc(100% - 24px)); margin: 16px auto 36px; }}
    .hero {{
      background: linear-gradient(135deg, {design.hero_start}, {design.hero_end});
      color: #f8f4ee;
      border-radius: 28px;
      padding: clamp(20px, 3vw, 30px);
      box-shadow: var(--shadow);
      position: relative;
      overflow: hidden;
    }}
    .hero::after {{
      content: "";
      position: absolute;
      inset: auto -40px -70px auto;
      width: 220px;
      height: 220px;
      background: radial-gradient(circle, rgba(239, 201, 167, 0.4), transparent 68%);
      transform: rotate(18deg);
    }}
    .eyebrow {{
      margin: 0 0 10px;
      color: rgba(248, 244, 238, 0.76);
      text-transform: uppercase;
      letter-spacing: 0.18em;
      font-size: 0.74rem;
    }}
    h1, h2, h3 {{ font-family: "Iowan Old Style", "Palatino Linotype", Georgia, serif; letter-spacing: 0.02em; margin: 0; }}
    h1 {{ font-size: clamp(2rem, 4vw, 3.2rem); }}
    .hero p {{ max-width: 860px; margin: 14px 0 0; color: rgba(248, 244, 238, 0.88); line-height: 1.6; }}
    .signal-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-top: 18px; }}
    .signal-card {{ padding: 12px 14px; border-radius: 18px; background: rgba(255, 255, 255, 0.08); backdrop-filter: blur(8px); min-width: 0; border: 1px solid rgba(255, 255, 255, 0.08); }}
    .signal-card span {{ display: block; font-size: 0.76rem; color: rgba(248, 244, 238, 0.68); }}
    .signal-card strong {{ display: block; margin-top: 4px; font-size: 1.02rem; word-break: break-word; }}
    .signal-card p {{ margin: 6px 0 0; color: rgba(248, 244, 238, 0.78); font-size: 0.84rem; line-height: 1.4; }}
    .hero-actions {{ display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin-top: 16px; }}
    .hero-label, .pill-link {{ display: inline-flex; align-items: center; justify-content: center; padding: 9px 12px; border-radius: 999px; background: rgba(255, 255, 255, 0.08); border: 1px solid rgba(255, 255, 255, 0.12); color: #f8f4ee; text-decoration: none; font-size: 0.86rem; font-weight: 700; }}
    .page-nav {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 14px; padding: 12px; border-radius: 20px; background: rgba(255, 251, 246, 0.78); border: 1px solid var(--line); box-shadow: var(--shadow); }}
    .page-nav a {{ padding: 9px 12px; border-radius: 999px; background: rgba(22, 48, 43, 0.05); color: var(--ink); font-size: 0.84rem; font-weight: 700; text-decoration: none; }}
    .status-label {{ margin: 0 0 8px; color: var(--accent); text-transform: uppercase; letter-spacing: 0.14em; font-size: 0.72rem; }}
    .timeline-card.current {{ border-color: rgba(214, 107, 52, 0.34); transform: translateY(-2px); }}
    .run-meta {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(128px, 1fr)); gap: 12px; margin-top: 22px; }}
    .run-meta div {{ padding: 12px 14px; border-radius: 16px; background: rgba(255, 255, 255, 0.08); backdrop-filter: blur(8px); min-width: 0; }}
    .run-meta span {{ display: block; font-size: 0.76rem; color: rgba(248, 244, 238, 0.68); }}
    .run-meta strong {{ display: block; margin-top: 4px; font-size: 1.02rem; word-break: break-word; }}
    .section {{ margin-top: 20px; }}
    .section-header {{ display: flex; flex-wrap: wrap; align-items: end; justify-content: space-between; gap: 12px; margin-bottom: 12px; }}
    .section-header p {{ margin: 0; color: var(--muted); line-height: 1.5; max-width: 760px; }}
    .metric-grid, .explainer-grid, .plot-grid, .info-grid, .guide-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 260px), 1fr)); gap: 14px; }}
    .card {{ background: var(--bg-panel); border: 1px solid var(--line); border-radius: 22px; box-shadow: var(--shadow); padding: 18px; min-width: 0; position: relative; overflow: hidden; }}
    .card::before {{ content: ""; position: absolute; inset: 0 auto auto 0; width: 100%; height: 4px; background: linear-gradient(90deg, var(--accent), rgba(214, 107, 52, 0.15)); }}
    .status-card {{ border-color: rgba(192, 96, 49, 0.22); background: linear-gradient(180deg, rgba(255, 250, 242, 0.95), rgba(255, 242, 233, 0.94)); }}
    .status-card h3 {{ margin-bottom: 10px; color: var(--bg-strong); }}
    .status-card p {{ margin: 0; color: var(--muted); line-height: 1.55; }}
    .metric-card strong {{ display: block; font-size: clamp(1.6rem, 3vw, 2rem); color: var(--bg-strong); margin-top: 10px; }}
    .metric-card span {{ color: var(--muted); font-size: 0.84rem; text-transform: uppercase; letter-spacing: 0.08em; }}
    .metric-card p {{ margin: 10px 0 0; color: var(--muted); line-height: 1.45; font-size: 0.93rem; }}
    .explainer-card h3 {{ margin-bottom: 10px; }}
    .explainer-card p {{ margin: 0; color: var(--muted); line-height: 1.55; }}
    .guide-card {{ padding-top: 14px; }}
    .guide-card summary {{ cursor: pointer; font-weight: 700; color: var(--bg-strong); list-style: none; }}
    .guide-card summary::-webkit-details-marker {{ display: none; }}
    .guide-card summary::after {{ content: "+"; float: right; color: var(--accent); font-weight: 700; }}
    .guide-card[open] summary::after {{ content: "-"; }}
    .guide-card p {{ margin: 10px 0 0; color: var(--muted); line-height: 1.55; }}
    .guide-card strong {{ color: var(--bg-strong); }}
    .plot-card img {{ width: 100%; display: block; border-radius: 16px; border: 1px solid var(--line); background: white; }}
    .plot-card h3 {{ margin-bottom: 12px; }}
    .table-shell {{ overflow-x: auto; margin-top: 8px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.94rem; min-width: 720px; }}
    th, td {{ text-align: left; padding: 11px 10px; border-bottom: 1px solid var(--line); vertical-align: top; }}
    th {{ font-size: 0.77rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); }}
    code {{ font-family: \"Consolas\", \"Courier New\", monospace; background: rgba(22, 48, 43, 0.06); padding: 0.15rem 0.35rem; border-radius: 0.35rem; word-break: break-word; }}
    .pill {{ display: inline-block; padding: 0.28rem 0.55rem; border-radius: 999px; font-size: 0.8rem; font-weight: 700; letter-spacing: 0.03em; white-space: nowrap; }}
    .pill-success {{ background: rgba(44, 106, 74, 0.14); color: var(--success); }}
    .pill-failure {{ background: rgba(143, 59, 46, 0.14); color: var(--failure); }}
    .empty-state {{ color: var(--muted); font-style: italic; }}
    .artifact-list {{ display: grid; gap: 10px; margin-top: 12px; }}
    .artifact-row {{ padding: 12px 14px; border-radius: 16px; background: rgba(22, 48, 43, 0.05); }}
    .artifact-row span {{ display: block; font-size: 0.76rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; }}
    .artifact-row code {{ display: block; margin-top: 6px; }}
    .footer-note {{ color: var(--muted); font-size: 0.92rem; margin-top: 12px; line-height: 1.5; }}
    .highlight {{ color: var(--accent); background: var(--accent-soft); padding: 0.12rem 0.4rem; border-radius: 999px; font-weight: 700; }}
    @media (max-width: 720px) {{
      .page {{ width: min(100% - 14px, 1280px); margin: 10px auto 24px; }}
      .card {{ padding: 14px; border-radius: 18px; }}
      .hero {{ border-radius: 20px; }}
      .run-meta {{ grid-template-columns: repeat(auto-fit, minmax(110px, 1fr)); }}
      table {{ min-width: 580px; font-size: 0.88rem; }}
      th, td {{ padding: 9px 8px; }}
    }}
  </style>
</head>
<body>
  <main class=\"page\">
    <section class=\"hero\">
      <p class="eyebrow">{escape(design.eyebrow)}</p>
      <h1>{escape(design.headline)}</h1>
      <p>{escape(design.subhead)}</p>
      <div class="hero-actions"><span class="hero-label">v{escape(current_release.version)} | {escape(current_release.code_name)}</span><span class="hero-label">Theme {escape(design.theme_name)}</span><a class="pill-link" href="version_history.html">Version History</a></div>
      <div class=\"signal-grid\">
        {signal_cards}
      </div>
      <div class=\"run-meta\"> 
        <div><span>Version</span><strong>{escape(str(summary_row.get("version", "unknown")))}</strong></div>
        <div><span>Mode</span><strong>{escape(mode)}</strong></div>
        <div><span>Budget</span><strong>{budget}</strong></div>
        <div><span>Seed</span><strong>{seed}</strong></div>
        <div><span>Task Length</span><strong>{task_length}</strong></div>
        <div><span>Train Episodes</span><strong>{int(summary_row.get("train_episodes", 0))}</strong></div>
        <div><span>Eval Episodes</span><strong>{int(summary_row.get("eval_episodes", 0))}</strong></div>
        <div><span>Eval Modulus</span><strong>{int(summary_row.get("eval_modulus", 0))}</strong></div>
        <div><span>Eval Bucket</span><strong>{int(summary_row.get("eval_bucket", 0))}</strong></div>
        <div><span>Max Chunk Size</span><strong>{max_chunk_size}</strong></div>
        <div><span>Promote Threshold</span><strong>{promote_threshold}</strong></div>
        <div><span>Max Vocabulary</span><strong>{max_vocabulary_size}</strong></div>
        <div><span>Memory Entry Cost</span><strong>{memory_entry_cost}</strong></div>
        <div><span>Run ID</span><strong>{escape(run_id)}</strong></div>
      </div>
    </section>

    <nav class="page-nav">
      <a href="#briefing">Executive Briefing</a>
      <a href="#history">Version Timeline</a>
      <a href="#policy">Policy Engine</a>
      <a href="#model">Operating Model</a>
      <a href="#summary">System Health</a>
      <a href="#eval">Held-Out Evaluation</a>
      <a href="#artifacts">Artifacts</a>
      <a href="#detail">Operational Detail</a>
    </nav>

    <section class="section" id="briefing">
      <div class="section-header">
        <div>
          <h2>Executive Briefing</h2>
          <p>This is the board-facing readout for the release: why the version exists, what changed, where the risk is, and what the team should do next.</p>
        </div>
      </div>
      <div class="info-grid">
        <article class="card status-card">
          <p class="status-label">{escape(design.status_label)}</p>
          <h3>{escape(design.status_title)}</h3>
          <p>{escape(design.status_body)}</p>
        </article>
        <article class="card">
          <h3>Release Direction</h3>
          <p>{escape(current_release.summary)}</p>
          <p class="footer-note"><span class="highlight">Focus:</span> {escape(current_release.focus)}</p>
          <p class="footer-note"><span class="highlight">Next:</span> {escape(design.next_move)}</p>
        </article>
      </div>
      <div class="metric-grid" style="margin-top:14px">
        {briefing_cards}
      </div>
    </section>

    <section class="section" id="history">
      <div class="section-header">
        <div>
          <h2>Version Timeline</h2>
          <p>This is the running release history for the compression program. It shows how the repo moved from MVP plumbing into reusable compression and now into board-facing observability.</p>
        </div>
      </div>
      <div class="metric-grid">
        {timeline_cards}
      </div>
    </section>

    <section class="section" id="policy">
      <div class="section-header">
        <div>
          <h2>Policy Engine</h2>
          <p>This release changes the policy objective as well as the policy behavior. The cards below show the live branch outcome, how much real intervention signal was collected, and whether those learned decisions beat the rule baseline on held-out eval.</p>
        </div>
      </div>
      <div class="metric-grid">
        {policy_cards}
      </div>
    </section>

    <section class="section" id="model">
      <div class="section-header">
        <div>
          <h2>Operating Model</h2>
          <p>These cards explain how the training branch, frozen eval, delta-based policy supervision, and design layer fit together so you can read the run without decoding the codebase first.</p>
        </div>
      </div>
      <div class="explainer-grid">
        <article class="card explainer-card">
          <h3>Training Phase</h3>
          <p>The sender is allowed to observe outputs and promote useful chunks into memory during the train split only.</p>
        </article>
        <article class="card explainer-card">
          <h3>Held-Out Eval</h3>
          <p>The eval split is deterministic and disjoint from the train split. No new chunks are learned there, so eval reflects reuse and generalization rather than fresh memorization.</p>
        </article>
        <article class="card explainer-card">
          <h3>Net Compression</h3>
          <p>Shorter messages alone are not enough. The dashboard also charges the system for retained memory, so positive net compression means the full codec is cheaper than literal transmission.</p>
        </article>
        <article class="card explainer-card">
          <h3>Live Promotion</h3>
          <p>When a learned policy is ready, the active branch can decide during training which chunks get a token immediately. A shadow rule branch replays the same train split so this dashboard can show whether ML changed behavior or just matched the heuristic.</p>
        </article>
        <article class="card explainer-card">
          <h3>Delta Supervision</h3>
          <p>The model is no longer trained only on whether a chunk appears useful on held-out targets. It now stores branch-delta signal, so unique learned decisions are rewarded only when the learned branch actually wins on the measured comparison.</p>
        </article>
        <article class="card explainer-card">
          <h3>Policy Dataset</h3>
          <p>Each run exports chunk-candidate rows with observations, usages, utilities, intervention types, and delta-weighted labels. That dataset is now grounded in branch comparison rather than only end-of-run pruning.</p>
        </article>
        <article class="card explainer-card">
          <h3>Design Agent</h3>
          <p>A deterministic design agent now shapes the board-facing copy, theme, and hero framing from the run state so this page works as a dashboard, documentation surface, and landing page.</p>
        </article>
      </div>
    </section>

    <section class="section" id="summary">
      <div class="section-header">
        <div>
          <h2>Overall Summary</h2>
          <p>High-level health and full-system cost across train and eval together. Use the guide below if you want the exact meaning of each box.</p>
        </div>
      </div>
      <div class="metric-grid">
        {summary_cards}
      </div>
    </section>

    <section class="section">
      <div class="section-header">
        <div>
          <h2>Summary Guide</h2>
          <p>This explains each overall summary box in plain English. Open any card for the meaning, rough formula, and what a good result looks like.</p>
        </div>
      </div>
      <div class="guide-grid">
        {summary_guide_cards}
      </div>
    </section>

    <section class="section" id="artifacts">
      <div class="section-header">
        <div>
          <h2>Artifacts</h2>
          <p>This page now doubles as the run landing page. These file pointers make the generated outputs easy to inspect and share during reviews.</p>
        </div>
      </div>
      <div class="info-grid">
        <article class="card">
          <h3>Artifact Index</h3>
          <div class="artifact-list">
            {artifact_rows}
          </div>
          <p class="footer-note">Open <code>{escape((dashboard_dir / 'latest_dashboard.html').as_posix())}</code> to keep pointing at the newest generated dashboard file.</p>
        </article>
      </div>
    </section>

    <section class="section" id="eval">
      <div class="section-header">
        <div>
          <h2>Held-Out Evaluation</h2>
          <p>The numbers in this section are the most important checkpoint for generalization because the memory is frozen and the tasks come from the eval split.</p>
        </div>
      </div>
      <div class="metric-grid">
        {eval_cards}
      </div>
    </section>

    <section class="section">
      <div class="section-header">
        <div>
          <h2>Plots</h2>
          <p>The orange boundary in each plot marks the point where training ended and held-out evaluation began.</p>
        </div>
      </div>
      <div class="plot-grid">
        {plot_cards}
      </div>
    </section>

    <section class="section info-grid" id="detail">
      <div class=\"card\">
        <div class=\"section-header\">
          <div>
            <h2>Task Breakdown</h2>
            <p>Which task families are compressing cleanly and which ones still need more literal fallback.</p>
          </div>
        </div>
        <div class=\"table-shell\">
          <table>
            <thead>
              <tr><th>Task</th><th>Episodes</th><th>Success Rate</th><th>Avg Message</th><th>Chunk Hits</th></tr>
            </thead>
            <tbody>
              {task_breakdown}
            </tbody>
          </table>
        </div>
      </div>
      <div class=\"card\">
        <div class=\"section-header\">
          <div>
            <h2>Retained Chunks</h2>
            <p>The chunk lexicon that survived pruning and will be available next run.</p>
          </div>
        </div>
        <div class=\"table-shell\">
          <table>
            <thead>
              <tr><th>Chunk</th><th>Token</th></tr>
            </thead>
            <tbody>
              {lexicon_rows}
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section class=\"section info-grid\">
      <div class=\"card\">
        <div class=\"section-header\">
          <div>
            <h2>Chunk Policy Data</h2>
            <p>Top chunk candidates exported for learned promotion and retention work. Intervention shows whether the learned branch made a unique decision or matched the rule baseline, and Train Delta shows whether that branch used the chunk more or less often during training.</p>
          </div>
        </div>
        <div class=\"table-shell\">
          <table>
            <thead>
              <tr><th>Chunk</th><th>Intervention</th><th>Score</th><th>Train Delta</th><th>Eval Hits</th><th>Generalizes</th><th>Retained</th></tr>
            </thead>
            <tbody>
              {policy_table_rows}
            </tbody>
          </table>
        </div>
      </div>
      <div class=\"card\">
        <div class=\"section-header\">
          <div>
            <h2>Most Common Messages</h2>
            <p>This shows the actual protocol forms that were sent most often, whether they were literal values or learned chunk tokens.</p>
          </div>
        </div>
        <div class=\"table-shell\">
          <table>
            <thead>
              <tr><th>Message</th><th>Count</th><th>Success Rate</th></tr>
            </thead>
            <tbody>
              {message_breakdown}
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section class=\"section info-grid\">
      <div class=\"card\">
        <div class=\"section-header\">
          <div>
            <h2>Failed Episodes</h2>
            <p>If failures appear, they usually mean the budget was too small for the needed literals or chunks were not yet available.</p>
          </div>
        </div>
        <div class=\"table-shell\">
          <table>
            <thead>
              <tr><th>Episode</th><th>Phase</th><th>Task</th><th>Input</th><th>Expected</th><th>Message</th><th>Decoded</th><th>Output</th><th>Result</th></tr>
            </thead>
            <tbody>
              {failed_episodes}
            </tbody>
          </table>
        </div>
      </div>
      <div class=\"card\">
        <div class=\"section-header\">
          <div>
            <h2>Recent Episodes</h2>
            <p>Latest transmissions in reverse chronological order, including whether the row came from training or frozen eval.</p>
          </div>
        </div>
        <div class=\"table-shell\">
          <table>
            <thead>
              <tr><th>Episode</th><th>Phase</th><th>Task</th><th>Input</th><th>Expected</th><th>Message</th><th>Decoded</th><th>Output</th><th>Result</th></tr>
            </thead>
            <tbody>
              {recent_episodes}
            </tbody>
          </table>
        </div>
        <p class=\"footer-note\">Open <code>{escape((dashboard_dir / 'latest_dashboard.html').as_posix())}</code> to keep pointing at the newest generated dashboard file.</p>
      </div>
    </section>
  </main>
</body>
</html>
"""


def _build_task_breakdown(episode_rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, dict[str, float]] = defaultdict(
        lambda: {"episodes": 0.0, "successes": 0.0, "message_length_total": 0.0, "dictionary_hits": 0.0}
    )

    for row in episode_rows:
        task_type = str(row["task_type"])
        grouped[task_type]["episodes"] += 1
        grouped[task_type]["successes"] += int(bool(row["success"]))
        grouped[task_type]["message_length_total"] += float(row["message_length"])
        grouped[task_type]["dictionary_hits"] += int(bool(row["dictionary_hit"]))

    breakdown = []
    for task_type, metrics in sorted(grouped.items()):
        episodes = int(metrics["episodes"])
        breakdown.append(
            {
                "task_type": task_type,
                "episodes": episodes,
                "success_rate": metrics["successes"] / episodes,
                "avg_message_length": metrics["message_length_total"] / episodes,
                "dictionary_hits": int(metrics["dictionary_hits"]),
            }
        )
    return breakdown


def _build_message_breakdown(episode_rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    counts: Counter[str] = Counter()
    success_counts: Counter[str] = Counter()

    for row in episode_rows:
        message = str(row["sender_message"] or "<empty>")
        counts[message] += 1
        success_counts[message] += int(bool(row["success"]))

    breakdown = []
    for message, count in counts.most_common():
        breakdown.append(
            {
                "message": message,
                "count": count,
                "success_rate": success_counts[message] / count,
            }
        )
    return breakdown


def _build_eval_cards(summary_row: Mapping[str, object], eval_episodes: int) -> str:
    if eval_episodes <= 0:
        return _render_metric_card(
            "Eval Disabled",
            "0",
            "No held-out evaluation episodes were configured for this run.",
        )

    cards = [
        _render_metric_card("Eval Success", _format_percent(float(summary_row["eval_success_rate"])), "How often the frozen codec reconstructed the exact correct answer on the held-out split."),
        _render_metric_card("Eval Chunk Hit Rate", _format_percent(float(summary_row["eval_dictionary_hit_rate"])), "How often held-out eval used at least one learned chunk token."),
        _render_metric_card("Eval Avg Message", _format_float(float(summary_row["eval_average_message_length"])), "Average held-out message length after training was complete."),
        _render_metric_card("Eval Net Ratio", _format_float(float(summary_row["eval_net_compression_ratio"])), "Held-out full-system compression ratio after charging the retained memory cost once."),
        _render_metric_card("Eval Net Savings", _format_signed_int(int(summary_row["eval_net_token_savings"])), "Held-out token savings after charging retained memory cost."),
        _render_metric_card("Message Gap", _format_float(float(summary_row["generalization_message_gap"])), "Eval average message length minus train average message length. Closer to zero is better."),
    ]
    return "\n".join(cards)


def _build_eval_status(
    eval_episodes: int,
    eval_net_ratio: float,
    eval_net_savings: int,
    overall_net_ratio: float,
    overall_net_savings: int,
) -> tuple[str, str]:
    if eval_episodes <= 0:
        return (
            "Held-Out Eval Is Disabled",
            "This run did not include a frozen eval split. You still have overall compression metrics, but you do not yet have a clean within-run generalization checkpoint.",
        )
    if eval_net_ratio < 1.0 and eval_net_savings > 0:
        return (
            "Held-Out Generalization Is Net Positive",
            "The frozen codec stayed accurate and still beat literal transmission on the held-out split after memory cost was counted. That is the strongest signal in the current system that learned structure is carrying over beyond the training stream.",
        )
    if overall_net_ratio < 1.0 and overall_net_savings > 0:
        return (
            "Training Helped, But Held-Out Eval Is Not Net Positive Yet",
            "The system is valuable on the whole run, but the held-out split still does not clear the full cost line. The next step is improving chunk selection, not just growing more memory.",
        )
    return (
        "Compression Is Still Early-Stage On Held-Out Eval",
        "The system may be learning useful chunks, but held-out eval is not yet paying for the retained dictionary. That is expected in early versions and is exactly why the eval split now exists.",
    )


def _build_policy_cards(summary_row: Mapping[str, object]) -> str:
    policy_ready = bool(int(summary_row.get("policy_model_ready", 0)))
    ready_next = bool(int(summary_row.get("policy_model_ready_next_run", 0)))
    active_retention_policy = str(summary_row.get("active_retention_policy", "not_applicable")).replace("_", " ").title()
    active_promotion_policy = str(summary_row.get("active_promotion_policy", "not_applicable")).replace("_", " ").title()
    training_signal = str(summary_row.get("policy_training_signal", "unknown")).replace("_", " ")
    model_status = "ready" if policy_ready else "cold start"
    next_status = "ready" if ready_next else "not ready"
    train_delta = float(summary_row.get("policy_train_message_gain_vs_rule", 0.0))
    eval_delta = float(summary_row.get("policy_eval_net_gain_vs_rule", 0.0))
    model_examples = int(summary_row.get("policy_model_examples_before_run", 0))
    appended_examples = int(summary_row.get("policy_history_rows_appended", 0))
    live_promotions = int(summary_row.get("policy_live_promotions", 0))
    live_rejections = int(summary_row.get("policy_live_rejections", 0))
    boundary_promotions = int(summary_row.get("policy_boundary_promotions", 0))
    intervention_rows = int(summary_row.get("policy_intervention_rows", 0))
    unique_learned_rows = int(summary_row.get("policy_unique_learned_rows", 0))
    positive_rows = int(summary_row.get("policy_delta_positive_rows", 0))
    negative_rows = int(summary_row.get("policy_delta_negative_rows", 0))
    cards = [
        _render_metric_card("Training Signal", training_signal.title(), "Which labeling rule the model is training against. Branch-delta means unique learned decisions only get rewarded when the learned branch actually wins."),
        _render_metric_card("Active Promotion", active_promotion_policy, "Which promotion policy shaped the live training branch in this run."),
        _render_metric_card("Active Retention", active_retention_policy, "Which retention policy produced the final eval memory and persisted dictionary."),
        _render_metric_card("Model Ready Now", model_status, "Whether a learned model existed before this run started. Cold start means training stayed on the rule path."),
        _render_metric_card("Model Ready Next Run", next_status, "Whether this run finished with enough labeled history to activate learned live promotion next time."),
        _render_metric_card("Train Message Gain", _format_float(train_delta), "Rule train average message length minus learned train average message length. Positive means the learned branch communicated more compactly during training."),
        _render_metric_card("Eval Gain Vs Rule", _format_float(eval_delta), "Rule eval net ratio minus learned eval net ratio. Positive means the learned branch beat the rule baseline on held-out eval."),
        _render_metric_card("Intervention Rows", str(intervention_rows), "How many chunk rows captured a real difference between the learned branch and the rule baseline."),
        _render_metric_card("Unique Learned Rows", str(unique_learned_rows), "How many chunk rows came from decisions the learned branch made that the rule baseline did not."),
        _render_metric_card("Delta Labels", f"{positive_rows}/{negative_rows}", "Positive and negative labels written under the branch-delta training objective for this run."),
        _render_metric_card("Live Promotions", str(live_promotions), "How many chunks the learned policy promoted during training before the branch reached frozen eval."),
        _render_metric_card("Live Rejections", str(live_rejections), "How many chunk candidates were seen often enough to consider but were rejected by the learned live policy."),
        _render_metric_card("Boundary Promotions", str(boundary_promotions), "How many additional chunks were promoted at the train/eval boundary after the full training evidence was available."),
        _render_metric_card("Model Examples", str(model_examples), "How many labeled chunk-history rows were available to train the learned policy before this run."),
        _render_metric_card("History Rows Added", str(appended_examples), "New labeled chunk rows written by this run for future policy training."),
    ]
    return "\n".join(cards)


def _summary_guide_specs() -> list[tuple[str, str, str, str]]:
    return [
        (
            "Success Rate",
            "This is the share of episodes where the receiver produced the exact expected output.",
            "correct episodes / total episodes",
            "For this project, anything below 100% usually means the protocol or budget is breaking correctness, which matters more than compression.",
        ),
        (
            "Chunk Hit Rate",
            "This is how often at least one learned chunk token appeared in the message instead of sending everything literally.",
            "episodes with chunk use / total episodes",
            "A rising hit rate means memory is being reused. A high hit rate is good only if accuracy stays high and net compression also improves.",
        ),
        (
            "Avg Message Length",
            "This is the average number of transmitted tokens per episode.",
            "total transmitted tokens / total episodes",
            "Lower is better, but only if the receiver stays exact. Shorter messages alone can be misleading if memory cost is ignored.",
        ),
        (
            "Net Compression Ratio",
            "This is the main cost metric for the whole codec, not just the message.",
            "(message tokens + retained memory cost) / literal baseline tokens",
            "Below 1.00 means the full system beats literal transmission. Above 1.00 means memory is still costing more than it saves.",
        ),
        (
            "Net Token Savings",
            "This is the absolute token gain or loss after charging the run for retained memory.",
            "literal baseline - (message tokens + retained memory cost)",
            "Positive is good. Negative means the codec is still a loss-maker even if some messages got shorter.",
        ),
        (
            "Memory Cost",
            "This is the penalty charged for keeping chunk memory after the run.",
            "persisted chunks x memory entry cost",
            "If this keeps rising faster than message cost falls, the codec is over-storing and needs a better retention policy.",
        ),
        (
            "Persisted And Loaded Chunks",
            "Loaded chunks are what the run started with. Persisted chunks are what survived pruning and will be available next time.",
            "counts before and after the run lifecycle",
            "You want useful reuse without uncontrolled growth. The important question is whether these retained chunks keep paying for themselves later.",
        ),
        (
            "Policy Rows And Stability",
            "Policy rows are the training examples exported for future ML. Stability shows whether the same output tends to map to the same message pattern.",
            "candidate chunk rows and repeated-output consistency",
            "More policy rows means more learning data. Higher stability usually means the protocol is behaving more like a reusable codec and less like a one-off shortcut.",
        ),
    ]


def _build_version_history_html(output_dir: Path) -> str:
    cards = "\n".join(
        _render_history_page_card(record, current_version_record().version)
        for record in sorted(VERSION_HISTORY, key=lambda item: tuple(int(part) for part in item.version.split(".")), reverse=True)
    )
    return f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>Helix - Version History</title>
  <style>
    :root {{ --bg: #f7f0e5; --panel: rgba(255, 251, 245, 0.9); --ink: #182320; --muted: #5c655f; --line: rgba(24, 35, 32, 0.1); --accent: #d66b34; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: \"Aptos\", \"Segoe UI\", sans-serif; color: var(--ink); background: linear-gradient(180deg, #f7f0e5 0%, #ebdfcf 100%); }}
    .page {{ width: min(1100px, calc(100% - 24px)); margin: 20px auto 32px; }}
    .hero, .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 28px; box-shadow: 0 18px 40px rgba(24, 35, 32, 0.06); }}
    .hero {{ padding: 26px; }}
    h1, h2, h3 {{ font-family: \"Iowan Old Style\", Georgia, serif; margin: 0; }}
    .hero p {{ color: var(--muted); line-height: 1.6; max-width: 760px; }}
    .back-link {{ display: inline-flex; margin-top: 14px; text-decoration: none; color: var(--accent); font-weight: 700; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 14px; margin-top: 18px; }}
    .card {{ padding: 18px; }}
    .card.current {{ border-color: rgba(214, 107, 52, 0.34); transform: translateY(-2px); }}
    small {{ text-transform: uppercase; letter-spacing: 0.12em; color: var(--muted); }}
    p, li {{ color: var(--muted); line-height: 1.55; }}
    ul {{ margin: 10px 0 0; padding-left: 18px; }}
  </style>
</head>
<body>
  <main class=\"page\">
    <section class=\"hero\">
      <small>Helix</small>
      <h1>Version History</h1>
      <p>This page is the running release history for the compression program. It shows what changed, why it mattered, and what each version prepared the team to do next.</p>
      <a class=\"back-link\" href=\"latest_dashboard.html\">Return To Latest Dashboard</a>
    </section>
    <section class=\"grid\">{cards}</section>
  </main>
</body>
</html>
"""




def _load_latest_csv_row(directory: Path, pattern: str) -> dict[str, str] | None:
    if not directory.exists():
        return None
    candidates = sorted(directory.glob(pattern))
    if not candidates:
        return None
    latest_path = candidates[-1]
    with latest_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return rows[-1] if rows else None


def _load_json_file(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _find_latest_run_dashboard_name(dashboard_dir: Path) -> str | None:
    if not dashboard_dir.exists():
        return None
    candidates = sorted(
        path
        for path in dashboard_dir.glob("*_dashboard.html")
        if path.name != "latest_dashboard.html"
    )
    return candidates[-1].name if candidates else None


def _where_we_are_text(
    legacy_summary: Mapping[str, str] | None,
    trace_summary: Mapping[str, str] | None,
    corpus_summary: Mapping[str, str] | None,
    gateway_status: Mapping[str, object] | None,
    archive_summary: Mapping[str, str] | None,
) -> str:
    if trace_summary is None:
        return "The product path has not written any trace benchmark output yet, so the repo is still only showing the legacy research track."
    corpus_sources = int(corpus_summary.get("source_count", 0)) if corpus_summary is not None else 0
    corpus_families = int(corpus_summary.get("source_family_count", 0)) if corpus_summary is not None else 0
    recommended_cap = (
        str(corpus_summary.get("recommended_max_active_templates", "n/a"))
        if corpus_summary is not None
        else str(trace_summary.get("recommended_max_active_templates", "n/a"))
    )
    positive_archive_families = int(archive_summary.get("positive_archive_gain_source_count", 0)) if archive_summary is not None else 0
    return (
        f"The real product path is now semantic packing for AI traces and handoffs. "
        f"The latest trace run still shows positive batch and streaming gains, and the latest corpus run covers {corpus_sources} exported trace file"
        f"{'' if corpus_sources == 1 else 's'} across {corpus_families} workload famil"
        f"{'ies' if corpus_families != 1 else 'y'}. "
        f"The current recommended active-template cap is {recommended_cap}, and the product now also has an archive-adapter path for storage benchmarking. "
        f"The latest archive run shows {positive_archive_families} archive-positive source family"
        f"{'' if positive_archive_families == 1 else 'ies'}. "
        f"The local gateway has accepted {int(gateway_status.get('ingested_batches', 0)) if gateway_status is not None else 0} live OTLP batch"
        f"{'' if gateway_status is not None and int(gateway_status.get('ingested_batches', 0)) == 1 else 'es'}. "
        f"The older sequence-policy path still exists, but it is now supporting R&D rather than defining the product."
    )


def _honest_review_text(
    legacy_summary: Mapping[str, str] | None,
    trace_summary: Mapping[str, str] | None,
    corpus_summary: Mapping[str, str] | None,
    gateway_status: Mapping[str, object] | None,
    archive_summary: Mapping[str, str] | None,
) -> str:
    trace_gain = int(float(trace_summary.get("stream_pack_gain_vs_windowed_raw_zlib", 0))) if trace_summary is not None else 0
    corpus_sources = int(corpus_summary.get("source_count", 0)) if corpus_summary is not None else 0
    corpus_families = int(corpus_summary.get("source_family_count", 0)) if corpus_summary is not None else 0
    legacy_eval = float(legacy_summary.get("eval_net_compression_ratio", 0.0)) if legacy_summary is not None else 0.0
    tuning_uplift = (
        float(corpus_summary.get("avg_recommended_gain_delta_vs_baseline", 0.0))
        if corpus_summary is not None
        else float(trace_summary.get("recommended_gain_delta_vs_baseline", 0.0)) if trace_summary is not None else 0.0
    )
    negative_archive_families = int(archive_summary.get("negative_archive_gain_source_count", 0)) if archive_summary is not None else 0
    return (
        f"The trace product path is real enough to release to design partners because replay works, memory is bounded, and the latest streaming gain is {trace_gain:+d} bytes. "
        f"But the corpus is still thin at {corpus_sources} exported trace file"
        f"{'' if corpus_sources == 1 else 's'} across {corpus_families} famil"
        f"{'ies' if corpus_families != 1 else 'y'}, gateway traffic is still only {int(gateway_status.get('ingested_batches', 0)) if gateway_status is not None else 0} accepted batch"
        f"{'' if gateway_status is not None and int(gateway_status.get('ingested_batches', 0)) == 1 else 'es'}, retention tuning uplift is currently {tuning_uplift:+.2f}, and the legacy ML policy track is still not frontier-level because its held-out eval ratio remains {legacy_eval:.2f} instead of beating 1.00."
        f" The archive path still has {negative_archive_families} disclosed negative case"
        f"{'' if negative_archive_families == 1 else 's'}, so the public claim must stay narrow and evidence-based."
    )


def _implications_text(
    trace_summary: Mapping[str, str] | None,
    corpus_summary: Mapping[str, str] | None,
    gateway_status: Mapping[str, object] | None,
    archive_summary: Mapping[str, str] | None,
) -> str:
    if trace_summary is None:
        return "There is no current product implication yet because the trace path has not been run."
    positive_archive_families = int(archive_summary.get("positive_archive_gain_source_count", 0)) if archive_summary is not None else 0
    return (
        "This can realistically become infrastructure that reduces storage and transport cost for machine-generated traces. "
        f"If gains continue on real exported AI workloads and archive-positive results keep repeating across at least {positive_archive_families} source families, the product becomes useful as a semantic pre-storage and archive layer beside existing observability stacks rather than a replacement for them."
    )


def _render_brief_card(card) -> str:
    return '<article class="card"><h3>{}</h3><p>{}</p></article>'.format(escape(card.title), escape(card.body))


def _render_timeline_card(record, current_version: str) -> str:
    current_class = " current" if record.version == current_version else ""
    achievements = "".join(f"<li>{escape(item)}</li>" for item in record.achievements)
    return f'<article class="card timeline-card{current_class}"><h3>v{escape(record.version)} | {escape(record.code_name)}</h3><p>{escape(record.summary)}</p><ul>{achievements}</ul><p><strong>Next:</strong> {escape(record.next_step)}</p></article>'


def _render_history_page_card(record, current_version: str) -> str:
    current_class = " current" if record.version == current_version else ""
    achievements = "".join(f"<li>{escape(item)}</li>" for item in record.achievements)
    return f'<article class="card{current_class}"><small>{escape(record.released_on)}</small><h3>v{escape(record.version)} | {escape(record.code_name)}</h3><p><strong>Focus:</strong> {escape(record.focus)}</p><p>{escape(record.summary)}</p><ul>{achievements}</ul><p><strong>Set Up Next:</strong> {escape(record.next_step)}</p></article>'




def _render_artifact_row(label: str, value: str) -> str:
    return (
        '<div class="artifact-row">'
        f'<span>{escape(label)}</span>'
        f'<code>{escape(value)}</code>'
        '</div>'
    )


def _render_program_prototype_card(title: str, status: str, body: str, operator_note: str) -> str:
    return (
        '<article class="card">'
        f'<h3>{escape(title)}</h3>'
        f'<p><strong>Status:</strong> {escape(status)}</p>'
        f'<p>{escape(body)}</p>'
        f'<p><strong>Why It Matters:</strong> {escape(operator_note)}</p>'
        '</article>'
    )


def _render_metric_card(label: str, value: str, description: str) -> str:
    return (
        '<article class="card metric-card">'
        f'<span>{escape(label)}</span>'
        f'<strong>{escape(value)}</strong>'
        f'<p>{escape(description)}</p>'
        '</article>'
    )


def _render_signal_card(signal) -> str:
    return (
        '<article class="signal-card">'
        f'<span>{escape(signal.label)}</span>'
        f'<strong>{escape(signal.value)}</strong>'
        f'<p>{escape(signal.detail)}</p>'
        '</article>'
    )


def _render_summary_guide_card(label: str, meaning: str, formula: str, watch_for: str) -> str:
    return (
        '<details class="card guide-card">'
        f'<summary>{escape(label)}</summary>'
        f'<p><strong>Meaning:</strong> {escape(meaning)}</p>'
        f'<p><strong>Formula:</strong> <code>{escape(formula)}</code></p>'
        f'<p><strong>What To Watch:</strong> {escape(watch_for)}</p>'
        '</details>'
    )


def _render_plot_card(plot_path: Path, dashboard_dir: Path) -> str:
    relative_path = Path(os.path.relpath(plot_path, dashboard_dir)).as_posix()
    title = plot_path.stem.split("_", 1)[1].replace("_", " ").title() if "_" in plot_path.stem else plot_path.stem
    return f'<article class="card plot-card"><h3>{escape(title)}</h3><img src="{escape(relative_path)}" alt="{escape(title)}"></article>'


def _render_task_row(row: Mapping[str, object]) -> str:
    return (
        "<tr>"
        f"<td><code>{escape(str(row['task_type']))}</code></td>"
        f"<td>{int(row['episodes'])}</td>"
        f"<td>{escape(_format_percent(float(row['success_rate'])))}</td>"
        f"<td>{escape(_format_float(float(row['avg_message_length'])))}</td>"
        f"<td>{int(row['dictionary_hits'])}</td>"
        "</tr>"
    )


def _render_message_row(row: Mapping[str, object]) -> str:
    return (
        "<tr>"
        f"<td><code>{escape(str(row['message']))}</code></td>"
        f"<td>{int(row['count'])}</td>"
        f"<td>{escape(_format_percent(float(row['success_rate'])))}</td>"
        "</tr>"
    )


def _render_policy_row(row: Mapping[str, object]) -> str:
    retained = bool(row.get("retained_after_prune", 0))
    pill_class = "pill pill-success" if retained else "pill pill-failure"
    pill_label = "Yes" if retained else "No"
    token = str(row.get("retained_token") or row.get("token") or "")
    return (
        "<tr>"
        f"<td><code>{escape(str(row['chunk']))}</code></td>"
        f"<td><code>{escape(token or '-')}</code></td>"
        f"<td>{int(row['observations'])}</td>"
        f"<td>{int(row['usages'])}</td>"
        f"<td>{int(row['support_utility'])}</td>"
        f"<td>{int(row['realized_utility'])}</td>"
        f"<td><span class=\"{pill_class}\">{pill_label}</span></td>"
        "</tr>"
    )


def _render_episode_row(row: Mapping[str, object]) -> str:
    success = bool(row["success"])
    pill_class = "pill pill-success" if success else "pill pill-failure"
    pill_label = "Success" if success else "Failure"
    return (
        "<tr>"
        f"<td>{int(row['episode'])}</td>"
        f"<td><code>{escape(str(row['phase']))}</code></td>"
        f"<td><code>{escape(str(row['task_type']))}</code></td>"
        f"<td><code>{escape(str(row['input']))}</code></td>"
        f"<td><code>{escape(str(row['expected_output']))}</code></td>"
        f"<td><code>{escape(str(row['sender_message'] or '<empty>'))}</code></td>"
        f"<td><code>{escape(str(row['decoded_output']))}</code></td>"
        f"<td><code>{escape(str(row['receiver_output']))}</code></td>"
        f"<td><span class=\"{pill_class}\">{pill_label}</span></td>"
        "</tr>"
    )


def _render_lexicon_row(sequence: str, token: str) -> str:
    return f"<tr><td><code>{escape(sequence)}</code></td><td><code>{escape(token)}</code></td></tr>"


def _format_float(value: float) -> str:
    return f"{value:.2f}"


def _format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def _format_signed_int(value: int) -> str:
    return f"{value:+d}"


def _build_program_landing_html(output_root: Path, latest_legacy_dashboard_name: str | None) -> tuple[str, str, str]:
    current_release = current_version_record()
    legacy_summary = _load_latest_csv_row(output_root / "metrics", "*_summary.csv")
    trace_summary = _load_latest_csv_row(output_root / "trace", "*_trace_summary.csv")
    release_summary = _load_json_file(_latest_path(output_root / "release", "*_public_release_summary.json"))
    gateway_status = _load_json_file(output_root / "gateway" / "status.json")
    sidecar_status = _load_json_file(output_root / "sidecar" / "status.json")
    demo_summary = _load_json_file(output_root / "sidecar" / "demo_result.json")
    partner_summary = _load_json_file(output_root / "partner_trials" / "latest_partner_trial_summary.json")
    internal_partner_summary = _load_json_file(output_root / "internal_partner" / "latest_internal_partner_summary.json")
    worker_summary = _load_json_file(output_root / "worker" / "latest_trace_worker_summary.json")
    dashboard_dir = output_root / "dashboard"
    legacy_dashboard_name = latest_legacy_dashboard_name or _find_latest_run_dashboard_name(dashboard_dir)

    public_context = release_summary or {
        "context": "public_corpus",
        "generated_at": "",
        "release_state": "Design-partner beta evidence still incomplete",
        "source_count": 0,
        "source_family_count": 0,
        "archive_bundle_gain_vs_source_gzip": 0,
        "positive_archive_gain_source_count": 0,
        "negative_archive_gain_source_count": 0,
        "replay_verified": 0,
        "reproducibility_verified": 0,
    }
    live_archive = dict(sidecar_status.get("archive", {})) if sidecar_status is not None else {}
    live_gateway = dict(sidecar_status.get("gateway", {})) if sidecar_status is not None else {}
    live_scorecard = dict(sidecar_status.get("scorecard", {})) if sidecar_status is not None else {}
    gateway_batches = int(live_gateway.get("ingested_batches", 0)) if live_gateway else int(gateway_status.get("ingested_batches", 0)) if gateway_status else 0
    gateway_mode = (
        "degraded"
        if (sidecar_status and bool(sidecar_status.get("degraded_mode", False))) or (gateway_status and bool(gateway_status.get("degraded_mode", False)))
        else ("healthy" if sidecar_status is not None or gateway_status is not None else "not started")
    )

    beta_gate = evaluate_beta_gate(
        public_context=public_context,
        partner_context=partner_summary,
        worker_context=worker_summary,
        output_root=output_root,
    )
    public_state = str(beta_gate.get("release_state", public_context.get("release_state", "Design-partner beta only")))
    public_launch_target = str(beta_gate.get("launch_target", public_context.get("launch_target", "Design-partner beta")))
    partner_recommendation = str(partner_summary.get("recommendation", "not run yet")) if partner_summary else "not run yet"
    worker_backend = str(worker_summary.get("storage_backend", "not configured")) if worker_summary else "not configured"
    launch_gate = str(beta_gate.get("launch_gate", "Run a partner trial on a real export."))
    next_commercial_step = str(
        beta_gate.get(
            "next_commercial_step",
            "Run a partner trial on a real exported AI trace set and use that result as the board-facing launch decision.",
        )
    )

    board_letter = build_board_letter(
        current_release=current_release,
        public_context=public_context,
        partner_context=partner_summary,
        internal_context=internal_partner_summary,
        launch_gate=launch_gate,
        next_commercial_step=next_commercial_step,
    )

    public_cards = "\n".join(
        [
            _render_metric_card("Context", str(public_context.get("context", "public_corpus")), "This block is the reproducible public-corpus context."),
            _render_metric_card("Generated", str(public_context.get("generated_at", "")) or "n/a", "When the current public-corpus summary was written."),
            _render_metric_card("Source Files", str(int(public_context.get("source_count", 0))), "How many files are in the current public-corpus evidence set."),
            _render_metric_card("Families", str(int(public_context.get("source_family_count", 0))), "How many workload families are in that public-corpus set."),
            _render_metric_card("Archive Bundle Gain", _format_signed_int(int(public_context.get("archive_bundle_gain_vs_source_gzip", 0))), "Replay-bundle gain versus source plus gzip for the public corpus."),
            _render_metric_card("Positive Families", str(int(public_context.get("positive_archive_gain_source_count", 0))), "How many public-corpus families beat source plus gzip."),
            _render_metric_card("Negative Cases", str(int(public_context.get("negative_archive_gain_source_count", 0))), "How many public-corpus families still lose and must stay disclosed."),
            _render_metric_card("Replay Verified", "yes" if int(public_context.get("replay_verified", 0)) else "no", "Whether exact canonical replay passed in the public-corpus context."),
            _render_metric_card("Release Ready", "yes" if int(beta_gate.get("public_beta_ready", 0)) else "no", "Whether Helix clears the current release gate."),
        ]
    )
    sample_cards = "\n".join(
        [
            _render_metric_card("Context", str(demo_summary.get("context", "sample_demo")) if demo_summary else "sample_demo", "Shipped sample evaluation context."),
            _render_metric_card("Generated", str(demo_summary.get("generated_at", "")) if demo_summary else "not run yet", "When the latest sample demo was written."),
            _render_metric_card("Recommendation", str(demo_summary.get("recommendation", "not run yet")) if demo_summary else "not run yet", "Current sample-demo recommendation."),
            _render_metric_card("Archive Gain", _format_signed_int(int(demo_summary.get("archive_bundle_gain_vs_source_gzip", 0))) if demo_summary else "0", "Archive result for the sample demo context."),
        ]
    )
    partner_cards = "\n".join(
        [
            _render_metric_card("Context", str(partner_summary.get("context", "partner_trial")) if partner_summary else "partner_trial", "Bring-your-own-trace beta evaluation context."),
            _render_metric_card("Generated", str(partner_summary.get("generated_at", "")) if partner_summary else "not run yet", "When the latest partner trial was written."),
            _render_metric_card("Recommendation", str(partner_summary.get("recommendation", "not run yet")) if partner_summary else "not run yet", "Current partner-trial recommendation."),
            _render_metric_card("Launch Gate", str(partner_summary.get("launch_gate", "run a partner trial")) if partner_summary else "run a partner trial", "Current board-facing go/no-go statement for partner evaluation."),
        ]
    )
    live_cards = "\n".join(
        [
            _render_metric_card("Context", "live_sidecar_session", "Current live sidecar session state."),
            _render_metric_card("Generated", str(sidecar_status.get("generated_at", "")) if sidecar_status else "not started", "When the sidecar last refreshed status."),
            _render_metric_card("Gateway Batches", str(gateway_batches), "How many OTLP batches the current live ingest surface has accepted."),
            _render_metric_card("Mode", gateway_mode, "Whether the live ingest surface is healthy or in fail-open degraded mode."),
            _render_metric_card("Live Archive Gain", _format_signed_int(int(live_archive.get("archive_bundle_gain_vs_source_gzip", 0))) if live_archive else "0", "Latest archive result inside the live session context."),
            _render_metric_card("Pilot-Ready Workflows", str(int(live_scorecard.get("pilot_ready_workflow_count", 0))), "How many workflows are currently marked pilot-ready in the live session."),
        ]
    )
    internal_cards = "\n".join(
        [
            _render_metric_card("Context", str(internal_partner_summary.get("context", "internal_partner_app")) if internal_partner_summary else "internal_partner_app", "First-party dogfood application context."),
            _render_metric_card("Generated", str(internal_partner_summary.get("generated_at", "")) if internal_partner_summary else "not run yet", "When the internal partner app last generated workload evidence."),
            _render_metric_card("Recommendation", str(internal_partner_summary.get("recommendation", "not run yet")) if internal_partner_summary else "not run yet", "Current recommendation on the first-party internal workload."),
            _render_metric_card("Archive Gain", _format_signed_int(int(internal_partner_summary.get("archive_bundle_gain_vs_source_gzip", 0))) if internal_partner_summary else "0", "Archive result for the internal-partner workload."),
            _render_metric_card("Generated Batches", str(int(internal_partner_summary.get("generated_batch_count", 0))) if internal_partner_summary else "0", "How many OTLP export batches the first-party app generated."),
            _render_metric_card("Sidecar Posts", str(int(internal_partner_summary.get("sidecar_posted_batches", 0))) if internal_partner_summary else "0", "How many generated batches reached the live EPL sidecar."),
        ]
    )
    artifact_cards = "\n".join(
        [
            _render_artifact_row("Public Site", "site/index.html"),
            _render_artifact_row("Latest Board Letter", "latest_board_letter.html"),
            _render_artifact_row("Latest Beta Gate", "latest_beta_gate.html" if (output_root / "dashboard" / "latest_beta_gate.html").exists() else "not generated yet"),
            _render_artifact_row("Public Release Brief", "latest_public_release_brief.html" if (output_root / "dashboard" / "latest_public_release_brief.html").exists() else "not generated yet"),
            _render_artifact_row("Latest Partner Trial", "latest_partner_trial.html" if (output_root / "dashboard" / "latest_partner_trial.html").exists() else "not generated yet"),
            _render_artifact_row("Internal Partner App", "latest_internal_partner_app.html" if (output_root / "dashboard" / "latest_internal_partner_app.html").exists() else "not generated yet"),
            _render_artifact_row("Latest Worker Report", "latest_trace_worker.html" if (output_root / "dashboard" / "latest_trace_worker.html").exists() else "not generated yet"),
            _render_artifact_row("Latest Archive Report", "latest_trace_archive.html" if (output_root / "dashboard" / "latest_trace_archive.html").exists() else "not generated yet"),
            _render_artifact_row("Latest Workflow Scorecard", "latest_trace_scorecard.html" if (output_root / "dashboard" / "latest_trace_scorecard.html").exists() else "not generated yet"),
            _render_artifact_row("Sidecar Status", "sidecar/status.json" if sidecar_status is not None else "not generated yet"),
            _render_artifact_row("Latest Legacy Run", legacy_dashboard_name or "no legacy run dashboard yet"),
        ]
    )
    trace_gain = _format_signed_int(int(trace_summary.get("stream_pack_gain_vs_windowed_raw_zlib", 0))) if trace_summary else "n/a"
    legacy_eval = _format_float(float(legacy_summary.get("eval_net_compression_ratio", 0.0))) if legacy_summary else "n/a"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="10">
  <title>Helix Dashboard</title>
  <style>
    :root {{ --bg: #f4efe6; --panel: rgba(255, 250, 242, 0.94); --ink: #1d2521; --muted: #5c655f; --line: rgba(22, 48, 43, 0.14); --accent: #c36135; --shadow: 0 18px 40px rgba(29, 37, 33, 0.08); }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Aptos", "Segoe UI Variable", "Segoe UI", sans-serif; color: var(--ink); background: linear-gradient(180deg, #f7f1e7 0%, #efe5d5 100%); }}
    .page {{ width: min(1320px, calc(100% - 24px)); margin: 16px auto 36px; }}
    .hero, .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 24px; box-shadow: var(--shadow); }}
    .hero {{ padding: 28px; background: linear-gradient(135deg, #15302b, #3d7267); color: #f8f4ee; }}
    .hero p {{ color: rgba(248, 244, 238, 0.88); line-height: 1.6; max-width: 900px; }}
    .hero-actions {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 16px; }}
    .pill {{ display: inline-flex; align-items: center; justify-content: center; padding: 9px 12px; border-radius: 999px; background: rgba(255,255,255,0.08); border: 1px solid rgba(255,255,255,0.12); color: #f8f4ee; text-decoration: none; font-weight: 700; font-size: 0.84rem; }}
    h1, h2, h3 {{ margin: 0; font-family: "Iowan Old Style", Georgia, serif; }}
    .section {{ margin-top: 20px; }}
    .section-header {{ margin-bottom: 12px; }}
    .section-header p {{ margin: 8px 0 0; color: var(--muted); line-height: 1.55; max-width: 820px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 260px), 1fr)); gap: 14px; }}
    .card {{ padding: 18px; }}
    .card p {{ color: var(--muted); line-height: 1.55; margin: 10px 0 0; }}
    .artifact-list {{ display: grid; gap: 10px; }}
    @media (max-width: 760px) {{ .page {{ width: min(100% - 14px, 1320px); margin: 10px auto 24px; }} .hero, .card {{ border-radius: 20px; padding: 16px; }} }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <p>Helix | v{escape(current_release.version)} | {escape(current_release.code_name)}</p>
      <h1>Program Dashboard</h1>
      <p>This is the board-facing control surface for the whole program. The contexts are separated on purpose: public-corpus evidence, shipped sample demo, bring-your-own-trace partner trial, and live sidecar session are all shown independently so the truth model stays stable.</p>
      <div class="hero-actions">
        <a class="pill" href="latest_board_letter.html">Board Letter</a>
        <a class="pill" href="version_history.html">Version History</a>
        <a class="pill" href="site/index.html">Public Site</a>
        <span class="pill">{escape(public_launch_target)}</span>
        <span class="pill">{escape(public_state)}</span>
        <span class="pill">Partner {escape(partner_recommendation)}</span>
        <span class="pill">Internal {escape(str(internal_partner_summary.get("recommendation", "not run yet")) if internal_partner_summary else "not run yet")}</span>
        <span class="pill">Worker {escape(worker_backend)}</span>
        <span class="pill">Trace gain {escape(trace_gain)}</span>
        <span class="pill">Legacy held-out net {escape(legacy_eval)}</span>
      </div>
    </section>
    <section class="section"><div class="section-header"><div><h2>Current State</h2><p>Reproducible external evidence only. This section is the Public corpus truth.</p></div></div><div class="grid">{public_cards}</div></section>
    <section class="section"><div class="section-header"><div><h2>Shipped sample demo</h2><p>Local sample run for onboarding and docs.</p></div></div><div class="grid">{sample_cards}</div></section>
    <section class="section"><div class="section-header"><div><h2>Partner trial</h2><p>Bring-your-own-trace beta result. This is the launch decision surface.</p></div></div><div class="grid">{partner_cards}</div></section>
    <section class="section"><div class="section-header"><div><h2>Internal partner app</h2><p>First-party dogfood evidence from our own trace-producing application. This strengthens our understanding, but it does not count as external partner proof.</p></div></div><div class="grid">{internal_cards}</div></section>
    <section class="section"><div class="section-header"><div><h2>Live sidecar session</h2><p>Current server state from the actual trial surface.</p></div></div><div class="grid">{live_cards}</div></section>
    <section class="section"><div class="section-header"><div><h2>Archive worker</h2><p>This is the production-shaped deployment path: exported traces in, replay bundle to object storage out.</p></div></div><div class="grid">
      <article class="card"><h3>Storage backend</h3><p>{escape(worker_backend)}</p></article>
      <article class="card"><h3>Stored bundle</h3><p><code>{escape(str(worker_summary.get("stored_bundle_uri", "not generated yet")) if worker_summary else "not generated yet")}</code></p></article>
      <article class="card"><h3>Worker verify</h3><p>{escape("passed" if worker_summary and int(worker_summary.get("bundle_verify_ok", 0)) else "not generated yet")}</p></article>
    </div></section>
    <section class="section"><div class="section-header"><div><h2>Release Readiness</h2><p>The current board recommendation is based on the shared release gate, not optimistic wording. Broad release stays blocked until public evidence, worker verification, and real external partner proof all align.</p></div></div><div class="grid">
      <article class="card"><h3>Launch Gate</h3><p>{escape(launch_gate)}</p></article>
      <article class="card"><h3>Next Commercial Step</h3><p>{escape(next_commercial_step)}</p></article>
      <article class="card"><h3>Blocking Reasons</h3><p>{escape('; '.join(str(item) for item in beta_gate.get("blocking_reason_details", [])) or 'No blockers.')}</p></article>
      <article class="card"><h3>Artifacts</h3><div class="artifact-list">{artifact_cards}</div></article>
    </div></section>
    <section class="section"><div class="section-header"><div><h2>Where We Are</h2><p>The shipping product is now the trace archive layer. The sequence track remains maintenance-only R&D.</p></div></div><div class="grid">
      <article class="card"><h3>Trace Product</h3><p>The board-facing product path is semantic archive and replay for AI traces, with explicit public-corpus evidence and a real design-partner beta workflow.</p></article>
      <article class="card"><h3>Launch Constraint</h3><p>The remaining bottleneck is not more synthetic R&D. It is partner-trial evidence on real exported traces that confirms or rejects the current wedge honestly.</p></article>
    </div></section>
    <section class="section"><div class="section-header"><div><h2>Honest Review</h2><p>The strongest value is still the narrow archive/replay wedge. The biggest remaining risk is claiming more than the evidence supports.</p></div></div><div class="grid">
      <article class="card"><h3>Implications</h3><p>If the partner-trial path stays positive on real exported AI traces, EPL becomes useful as a storage-layer component beside an existing observability stack rather than another observability UI.</p></article>
      <article class="card"><h3>Real-World Comparison</h3><p>Phoenix, OpenLIT, and similar tools own the observability workflow. EPL is the semantic archive and replay layer that can sit underneath or beside those systems when cold storage and replay cost matter.</p></article>
      <article class="card"><h3>Why This Is Different</h3><p>The product claim is not generic compression. It is exact-replay semantic packing for AI trace archives, with a design-partner workflow that answers whether the wedge works on a real export.</p></article>
    </div></section>
    <section class="section"><div class="section-header"><div><h2>Working Prototypes</h2><p>These are the concrete operator paths that exist in the repo today.</p></div></div><div class="grid">
      <article class="card"><h3>Public Alpha Docs</h3><p>The static site explains the wedge, the evidence boundary, and the setup path for new evaluators.</p></article>
      <article class="card"><h3>First-Party Pilot App</h3><p>A local workload generator now acts as our own first design partner, creating repeated AI support and ops traffic that EPL can archive, replay, and score without pretending it is external data.</p></article>
      <article class="card"><h3>Live Sidecar Trial</h3><p>The server at <code>/</code> accepts OTLP JSON, runs the shipped sample, and supports one-file or zipped-export analysis for design-partner beta evaluations.</p></article>
      <article class="card"><h3>Archive Worker</h3><p>The worker processes exported traces, writes replay bundles to configured storage, and gives operators a real retention-tier deployment path.</p></article>
      <article class="card"><h3>Offline Partner Trial</h3><p>The partner-trial CLI runs the same archive, corpus, and scorecard path on a user-provided export and emits a board-ready recommendation report.</p></article>
    </div></section>
    <section class="section"><div class="section-header"><div><h2>Letter To The Board</h2><p>The board letter is now generated from the same context snapshots shown above.</p></div></div><div class="grid">
      <article class="card"><h3>{escape(board_letter.title)}</h3><p>{escape(board_letter.paragraph_one)}</p><p>{escape(board_letter.paragraph_two)}</p></article>
      <article class="card"><h3>Why this matters</h3><p>The product can now answer a design-partner question directly: bring your own export, run EPL locally, and get a recommendation that is tied to a single context instead of a blended metric pool.</p></article>
    </div></section>
  </main>
</body>
</html>
"""
    board_letter_html = build_board_letter_html(board_letter, current_release=current_release)
    board_letter_markdown = build_board_letter_markdown(board_letter, current_release=current_release)
    return html, board_letter_html, board_letter_markdown


def _latest_path(directory: Path, pattern: str) -> Path:
    if not directory.exists():
        return directory / "__missing__"
    candidates = sorted(directory.glob(pattern))
    return candidates[-1] if candidates else directory / "__missing__"
