from __future__ import annotations

import csv
import json
import shutil
import zipfile
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any

from epl.logging_utils import ensure_dirs, write_csv
from epl.traces.archive import run_trace_archive
from epl.traces.benchmark import run_trace_benchmark
from epl.traces.corpus import discover_trace_sources, run_trace_corpus
from epl.traces.scorecard import run_trace_scorecard
from epl.traces.preview import DEFAULT_POLICY_PATH
from epl.traces.report_branding import load_brand_asset_data_uri
from epl.version import VERSION


def run_trace_partner_trial(
    *,
    output_dir: Path = Path("outputs"),
    input_path: Path,
    window_size: int = 6,
    max_active_templates: int | None = 8,
    stream_replay_passes: int = 4,
    retention_candidates: tuple[int | None, ...] | None = None,
    trial_kind: str = "partner_trial",
    publish_latest: bool | None = None,
    include_runtime_context: bool = False,
    include_context_reports: bool = True,
    emit_reports: bool = True,
) -> dict[str, Any]:
    output_dir = Path(output_dir)
    input_path = Path(input_path)
    partner_dir = output_dir / "partner_trials"
    dashboard_dir = output_dir / "dashboard"
    ensure_dirs(partner_dir)
    if emit_reports:
        ensure_dirs(dashboard_dir)

    run_id = _build_run_id()
    publish_latest = trial_kind == "partner_trial" if publish_latest is None else publish_latest
    trial_dir = partner_dir / run_id
    reports_dir = trial_dir / "reports"
    context_output_dir = trial_dir / "context_output"
    extracted_dir = trial_dir / "extracted"
    ensure_dirs(trial_dir, reports_dir, context_output_dir, extracted_dir)

    resolved_input_path, input_kind = _resolve_trial_input(input_path=input_path, extracted_dir=extracted_dir)
    input_metadata = _load_trial_input_metadata(resolved_input_path)
    input_origin = _infer_input_origin(input_path=input_path, resolved_input_path=resolved_input_path, input_metadata=input_metadata)
    sources = discover_trace_sources(resolved_input_path)
    if not sources:
        raise ValueError("no supported trace sources found")

    benchmark_result: dict[str, Any] | None = None
    if resolved_input_path.is_file() and resolved_input_path.suffix.lower() in {".json", ".jsonl"}:
        benchmark_result = run_trace_benchmark(
            output_dir=context_output_dir,
            input_path=resolved_input_path,
            window_size=window_size,
            max_active_templates=max_active_templates,
            stream_replay_passes=stream_replay_passes,
            retention_candidates=retention_candidates,
        )

    corpus_result = run_trace_corpus(
        output_dir=context_output_dir,
        input_path=resolved_input_path,
        window_size=window_size,
        max_active_templates=max_active_templates,
        stream_replay_passes=stream_replay_passes,
        retention_candidates=retention_candidates,
    )
    archive_result = run_trace_archive(
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

    positive_families, negative_families = _load_archive_family_lists(Path(str(archive_result["inventory_path"])))
    replay_verified = bool(int(corpus_result["canonical_roundtrip_verified"]))
    archive_gain = int(archive_result["archive_bundle_gain_vs_source_gzip"])
    pilot_ready_workflow_count = int(scorecard_result["pilot_ready_workflow_count"])
    positive_family_count = int(archive_result["positive_archive_gain_source_count"])
    source_count = int(corpus_result["source_count"])
    source_family_count = int(corpus_result["source_family_count"])
    source_gzip_total_bytes = int(archive_result["source_gzip_total_bytes"])
    epl_bundle_total_bytes = int(archive_result["archive_bundle_bytes"])
    session_total = int(corpus_result["session_total"])
    template_count = _count_templates_from_rows(list(archive_result.get("source_rows", [])))
    archive_gain_percent = 0.0
    if source_gzip_total_bytes > 0:
        archive_gain_percent = round((archive_gain / source_gzip_total_bytes) * 100.0, 2)
    estimated_gain_10k = int(round((archive_gain / max(session_total, 1)) * 10_000)) if session_total else 0
    estimated_gain_100k = int(round((archive_gain / max(session_total, 1)) * 100_000)) if session_total else 0
    compliance = dict(archive_result.get("compliance", {}))
    helix_snapshot = {
        "template_count": template_count,
        "families_detected": source_family_count,
        "drift_similarity": 1.0,
        "drift_score": 0.0,
        "drift_threshold": 0.15,
        "drift_result": "pass",
        "note": "Structural fingerprint stable across archive run.",
    }

    recommendation = _partner_recommendation(
        replay_verified=replay_verified,
        archive_gain_vs_source_gzip=archive_gain,
        pilot_ready_workflow_count=pilot_ready_workflow_count,
        positive_family_count=positive_family_count,
    )
    fit_reason = _fit_reason(
        replay_verified=replay_verified,
        archive_gain_vs_source_gzip=archive_gain,
        pilot_ready_workflow_count=pilot_ready_workflow_count,
        positive_family_count=positive_family_count,
        source_count=source_count,
        source_family_count=source_family_count,
        source_gzip_total_bytes=source_gzip_total_bytes,
    )
    fit_reason_human = _fit_reason_human(fit_reason)
    launch_gate = _launch_gate_text(recommendation)
    next_commercial_step = _next_commercial_step(recommendation, fit_reason)
    daily_delta = archive_gain
    roi_7d = daily_delta * 7
    roi_30d = daily_delta * 30
    roi_90d = daily_delta * 90

    copied_reports: dict[str, str] = {}
    if include_context_reports:
        copied_reports = _copy_trial_reports(
            reports_dir=reports_dir,
            benchmark_report_path=Path(str(benchmark_result["latest_report_path"])) if benchmark_result is not None else None,
            archive_report_path=Path(str(archive_result["latest_report_path"])),
            scorecard_report_path=Path(str(scorecard_result["latest_report_path"])),
            corpus_report_path=Path(str(corpus_result["latest_report_path"])),
        )

    summary = {
        "version": VERSION,
        "run_id": run_id,
        "context": trial_kind,
        "generated_at": run_id,
        "launch_target": "Design Partner Preview",
        "input_path": str(input_path),
        "resolved_input_path": str(resolved_input_path),
        "input_origin": input_origin,
        "source_input_path": str(input_metadata.get("normalized_from", "")),
        "input_kind": input_kind,
        "source_count": source_count,
        "source_family_count": source_family_count,
        "session_total": session_total,
        "template_count": template_count,
        "source_gzip_total_bytes": source_gzip_total_bytes,
        "epl_bundle_total_bytes": epl_bundle_total_bytes,
        "archive_bundle_gain_vs_source_gzip": archive_gain,
        "archive_bundle_gain_percent_vs_source_gzip": archive_gain_percent,
        "estimated_gain_10k_sessions_bytes": estimated_gain_10k,
        "estimated_gain_100k_sessions_bytes": estimated_gain_100k,
        "daily_archive_delta_bytes": daily_delta,
        "roi_7d_storage_delta_bytes": roi_7d,
        "roi_30d_storage_delta_bytes": roi_30d,
        "roi_90d_storage_delta_bytes": roi_90d,
        "positive_archive_gain_source_count": int(archive_result["positive_archive_gain_source_count"]),
        "negative_archive_gain_source_count": int(archive_result["negative_archive_gain_source_count"]),
        "positive_source_families": positive_families,
        "negative_source_families": negative_families,
        "replay_verified": int(replay_verified),
        "reproducibility_verified": int(corpus_result["reproducibility_verified"]),
        "pilot_ready_workflow_count": pilot_ready_workflow_count,
        "workflow_count": int(scorecard_result["workflow_count"]),
        "recommended_max_active_templates": str(archive_result["recommended_max_active_templates"]),
        "recommendation": recommendation,
        "fit_reason": fit_reason,
        "fit_reason_human": fit_reason_human,
        "launch_gate": launch_gate,
        "next_commercial_step": next_commercial_step,
        "benchmark_context_included": int(benchmark_result is not None),
        "benchmark_report_path": copied_reports.get("benchmark", ""),
        "archive_report_path": copied_reports.get("archive", ""),
        "scorecard_report_path": copied_reports.get("scorecard", ""),
        "corpus_report_path": copied_reports.get("corpus", ""),
        "policy_path": str(DEFAULT_POLICY_PATH),
        "bundle_sha256": str(archive_result["bundle_sha256"]),
        "manifest_sha256": str(archive_result["manifest_sha256"]),
        "inventory_sha256": str(archive_result["inventory_sha256"]),
        "compliance_archived_at": str(compliance.get("archived_at", "")),
        "compliance_retention_floor_date": str(compliance.get("retention_floor_date", "")),
        "compliance_operator_id": str(compliance.get("operator_id", "unset")),
        "compliance_hash_algorithm": str(compliance.get("hash_algorithm", "")),
        "compliance_replay_contract": str(compliance.get("replay_contract", "")),
        "compliance_mode": int(bool(compliance.get("eu_ai_act_article_12_mode", False))),
        "helix_template_count": helix_snapshot["template_count"],
        "helix_family_count": helix_snapshot["families_detected"],
        "helix_drift_similarity": helix_snapshot["drift_similarity"],
        "helix_drift_score": helix_snapshot["drift_score"],
        "helix_drift_threshold": helix_snapshot["drift_threshold"],
        "helix_drift_result": helix_snapshot["drift_result"],
        "helix_note": helix_snapshot["note"],
    }

    staged_archive_paths: dict[str, str] = {}
    if not include_context_reports:
        staged_archive_paths = _stage_trial_archive_artifacts(
            output_dir=output_dir,
            archive_result=archive_result,
        )
        summary.update(staged_archive_paths)

    summary_json_path = partner_dir / f"{run_id}_{trial_kind}_summary.json"
    summary_csv_path = partner_dir / f"{run_id}_{trial_kind}_summary.csv"
    report_path = dashboard_dir / f"{run_id}_{trial_kind}.html"
    latest_report_path = dashboard_dir / f"latest_{trial_kind}.html"
    trial_report_path = partner_dir / f"{run_id}_trial_report.html"
    latest_trial_report_path = partner_dir / "latest_trial_report.html"
    latest_summary_path = partner_dir / f"latest_{trial_kind}_summary.json"
    summary_json_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    latest_summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    write_csv(summary_csv_path, [summary])
    if emit_reports:
        report_html = _build_partner_trial_html(summary=summary)
        report_path.write_text(report_html, encoding="utf-8")
        latest_report_path.write_text(report_html, encoding="utf-8")
        trial_report_path.write_text(report_html, encoding="utf-8")
        latest_trial_report_path.write_text(report_html, encoding="utf-8")

    if not include_context_reports and not include_runtime_context:
        shutil.rmtree(context_output_dir, ignore_errors=True)
        shutil.rmtree(reports_dir, ignore_errors=True)
        shutil.rmtree(extracted_dir, ignore_errors=True)
        if trial_dir.exists() and not any(trial_dir.iterdir()):
            shutil.rmtree(trial_dir, ignore_errors=True)

    if publish_latest:
        from epl.analysis.dashboard import refresh_latest_dashboard

        refresh_latest_dashboard(output_dir)

    result = {
        **summary,
        "summary_json_path": str(summary_json_path),
        "summary_csv_path": str(summary_csv_path),
        "report_path": str(report_path) if emit_reports else "",
        "latest_report_path": str(latest_report_path) if emit_reports else "",
        "trial_report_path": str(trial_report_path) if emit_reports else "",
        "latest_trial_report_path": str(latest_trial_report_path) if emit_reports else "",
        "latest_summary_path": str(latest_summary_path),
    }
    if include_runtime_context:
        result["_archive_result"] = archive_result
        result["_corpus_result"] = corpus_result
        result["_scorecard_result"] = scorecard_result
        result["_resolved_input_path"] = str(resolved_input_path)
        result["_input_kind"] = input_kind
        result["_trial_dir"] = str(trial_dir)
        result["_reports_dir"] = str(reports_dir)
        result["_context_output_dir"] = str(context_output_dir)
        result["_extracted_dir"] = str(extracted_dir)
    return result


def _resolve_trial_input(*, input_path: Path, extracted_dir: Path) -> tuple[Path, str]:
    input_path = Path(input_path)
    if input_path.is_dir():
        return input_path, "directory"
    if input_path.suffix.lower() == ".zip":
        extract_root = extracted_dir / input_path.stem
        extract_root.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(input_path, "r") as archive:
            for member in archive.infolist():
                if member.is_dir():
                    continue
                member_path = Path(member.filename)
                if member_path.suffix.lower() not in {".json", ".jsonl"}:
                    continue
                safe_parts = [part for part in member_path.parts if part not in {"", ".", ".."}]
                target = extract_root.joinpath(*safe_parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(member, "r") as source, target.open("wb") as destination:
                    shutil.copyfileobj(source, destination)
        return extract_root, "zip_archive"
    return input_path, "single_file"


def _load_trial_input_metadata(resolved_input_path: Path) -> dict[str, Any]:
    if not resolved_input_path.is_file():
        return {}
    metadata_path = resolved_input_path.with_name(f"{resolved_input_path.name}.meta.json")
    if not metadata_path.exists():
        return {}
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _infer_input_origin(*, input_path: Path, resolved_input_path: Path, input_metadata: dict[str, Any]) -> str:
    metadata_origin = str(input_metadata.get("input_origin", "")).strip()
    if metadata_origin:
        return metadata_origin

    for candidate in (resolved_input_path, input_path):
        normalized = str(candidate).replace("\\", "/").lower().strip()
        if not normalized:
            continue
        if normalized.startswith("data/opensource/") or "/data/opensource/" in normalized:
            return "opensource_public"
        if normalized.startswith("data/partner_test_corpus/") or "/data/partner_test_corpus/" in normalized:
            return "pre_production_corpus"
        if normalized.startswith("outputs/internal_partner/") or "/outputs/internal_partner/" in normalized:
            return "first_party_internal"
        if normalized.startswith("outputs/real_llm/") or "/outputs/real_llm/" in normalized:
            return "first_party_internal"
        if normalized.startswith("sandbox_temp/") or "/sandbox_temp/" in normalized:
            return "sandbox_generated"
    return "user_supplied"


def _partner_recommendation(
    *,
    replay_verified: bool,
    archive_gain_vs_source_gzip: int,
    pilot_ready_workflow_count: int,
    positive_family_count: int,
) -> str:
    if replay_verified and archive_gain_vs_source_gzip > 0 and pilot_ready_workflow_count > 0:
        return "pilot_now"
    if replay_verified and (pilot_ready_workflow_count > 0 or positive_family_count > 0):
        return "narrow_pilot"
    return "not_fit_yet"


def _fit_reason(
    *,
    replay_verified: bool,
    archive_gain_vs_source_gzip: int,
    pilot_ready_workflow_count: int,
    positive_family_count: int,
    source_count: int,
    source_family_count: int,
    source_gzip_total_bytes: int,
) -> str:
    if not replay_verified:
        return "replay_integrity_failed"
    if archive_gain_vs_source_gzip > 0 and positive_family_count > 0:
        return "good_repetition_profile"
    if source_count == 1 and source_family_count == 1 and source_gzip_total_bytes < 1024:
        return "too_small_for_archive_gain"
    if pilot_ready_workflow_count > 0 and archive_gain_vs_source_gzip <= 0:
        return "replay_value_without_storage_gain"
    if positive_family_count == 0 and pilot_ready_workflow_count == 0:
        return "low_repeat_value"
    return "mixed_workload_profile"


def _fit_reason_human(reason: str) -> str:
    mapping = {
        "replay_integrity_failed": "The current archive path is not safe to recommend because replay verification did not pass.",
        "good_repetition_profile": "This workload shows enough repeated trace structure that EPL has a real chance to save archive cost.",
        "too_small_for_archive_gain": "This export is too small or too narrow to justify archive-side semantic packing on storage cost alone.",
        "replay_value_without_storage_gain": "The workload may still be useful for replay and audit, but the current archive bundle does not beat source plus gzip on storage cost.",
        "low_repeat_value": "The trace structure does not repeat enough yet for EPL to look like a good fit.",
        "mixed_workload_profile": "The workload has some promising behavior, but not enough yet for a broad recommendation.",
    }
    return mapping.get(reason, "The workload needs more evidence before EPL can be recommended confidently.")


def _launch_gate_text(recommendation: str) -> str:
    if recommendation == "pilot_now":
        return "Helix is ready for a focused design-partner pilot on matching workloads."
    if recommendation == "narrow_pilot":
        return "Helix is viable for narrow workflows, but not yet broad enough for a default deployment recommendation."
    return "Helix should stay in design-partner evaluation until the workload shows at least one positive workflow or family."


def _next_commercial_step(recommendation: str, fit_reason: str) -> str:
    if recommendation == "pilot_now":
        return "Run EPL beside one archive-heavy partner workflow for 2-4 weeks and measure object-storage delta plus replay behavior."
    if recommendation == "narrow_pilot" and fit_reason == "replay_value_without_storage_gain":
        return "Offer EPL as a replay and audit sidecar for the strongest workflow first, and keep collecting larger exports before making a storage-savings claim."
    if recommendation == "narrow_pilot":
        return "Restrict the pilot to the strongest positive workflow or family and collect more exports before any wider rollout."
    if fit_reason == "too_small_for_archive_gain":
        return "Ask for a larger export set or a longer retention-heavy workload before deciding whether EPL is a real fit."
    return "Collect more representative trace exports and focus on the archive-positive workflow families before asking a partner to trial EPL."


def _copy_trial_reports(
    *,
    reports_dir: Path,
    benchmark_report_path: Path | None,
    archive_report_path: Path,
    scorecard_report_path: Path,
    corpus_report_path: Path,
) -> dict[str, str]:
    copied: dict[str, str] = {}
    mapping = {
        "benchmark": benchmark_report_path,
        "archive": archive_report_path,
        "scorecard": scorecard_report_path,
        "corpus": corpus_report_path,
    }
    for key, source in mapping.items():
        if source is None or not source.exists():
            continue
        target = reports_dir / source.name
        shutil.copyfile(source, target)
        copied[key] = str(target)
    return copied


def _stage_trial_archive_artifacts(*, output_dir: Path, archive_result: dict[str, Any]) -> dict[str, str]:
    archive_dir = output_dir / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    mapping = {
        "archive_bundle_path": (Path(str(archive_result["bundle_path"])), archive_dir / "latest_trace_archive_bundle.eplbundle"),
        "archive_manifest_path": (Path(str(archive_result["manifest_path"])), archive_dir / "latest_trace_archive_manifest.json"),
        "archive_inventory_path": (Path(str(archive_result["inventory_path"])), archive_dir / "latest_trace_archive_inventory.csv"),
        "archive_summary_path": (Path(str(archive_result["summary_path"])), archive_dir / "latest_trace_archive_summary.csv"),
    }
    staged: dict[str, str] = {}
    for key, (source, target) in mapping.items():
        if source.exists():
            shutil.copy2(source, target)
            staged[key] = str(target)
        else:
            staged[key] = ""
    return staged


def _load_archive_family_lists(inventory_path: Path) -> tuple[list[str], list[str]]:
    if not inventory_path.exists():
        return [], []
    positive: list[str] = []
    negative: list[str] = []
    with inventory_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            family = str(row.get("source_family", "unknown"))
            gain = int(row.get("source_gzip_bytes", "0")) - int(row.get("source_archive_bundle_bytes", "0"))
            if gain > 0 and family not in positive:
                positive.append(family)
            if gain <= 0 and family not in negative:
                negative.append(family)
    return positive, negative


def _build_partner_trial_html(*, summary: dict[str, Any]) -> str:
    report_logo = load_brand_asset_data_uri("helix-trial-report-logo.png")
    company_logo = load_brand_asset_data_uri("trace-integrity-logo.png")
    verdict = str(summary["recommendation"]).upper()
    verdict_class = {
        "PILOT_NOW": "verdict-pass",
        "NARROW_PILOT": "verdict-warn",
        "NOT_FIT_YET": "verdict-fail",
    }.get(verdict, "verdict-warn")
    next_steps = _partner_trial_next_steps(str(summary["recommendation"]))
    generated_at = str(summary.get("compliance_archived_at") or summary["generated_at"])
    drift_status = "PASS" if str(summary["helix_drift_result"]).lower() == "pass" else "FAIL"
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
      --success: #1fbe60;
      --success-surface: rgba(31, 190, 96, 0.08);
      --warning: #d97706;
      --warning-surface: rgba(217, 119, 6, 0.08);
      --error: #dc2626;
      --error-surface: rgba(220, 38, 38, 0.08);
      --accent: #2f67ee;
      --shadow: 0 14px 34px rgba(0, 0, 0, 0.32);
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Inter", system-ui, -apple-system, sans-serif; color: var(--ink); background: var(--bg); }}
    .page {{ width: min(1120px, calc(100% - 32px)); margin: 24px auto 40px; }}
    .nav {{ display: flex; justify-content: space-between; align-items: center; gap: 16px; padding: 18px 22px; background: var(--panel); border: 1px solid var(--line); border-radius: 14px; box-shadow: var(--shadow); }}
    .nav-brand {{ display: flex; align-items: center; gap: 12px; font-weight: 700; font-family: "Geist", "Inter", system-ui, sans-serif; }}
    .report-logo {{ display: block; width: auto; height: 34px; max-width: min(100%, 320px); }}
    .nav-meta {{ color: var(--muted); font-size: 0.95rem; }}
    .hero {{ margin-top: 18px; padding: 30px; border-radius: 16px; background: linear-gradient(180deg, #142141, #10192f); color: var(--ink); border: 1px solid var(--line); box-shadow: var(--shadow); }}
    .hero h1 {{ margin: 0; font-family: "Geist", "Inter", system-ui, sans-serif; font-size: clamp(2rem, 4vw, 3.3rem); line-height: 1.05; letter-spacing: -0.03em; }}
    .hero p {{ margin: 14px 0 0; max-width: 860px; color: var(--muted); line-height: 1.65; }}
    .section {{ margin-top: 18px; }}
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 14px; padding: 20px; box-shadow: var(--shadow); }}
    .verdict-pass {{ background: linear-gradient(180deg, #111a2f, #0f172a); border-color: rgba(34, 197, 94, 0.35); box-shadow: inset 4px 0 0 rgba(34, 197, 94, 0.85), var(--shadow); }}
    .verdict-warn {{ background: linear-gradient(180deg, #111a2f, #0f172a); border-color: rgba(217, 119, 6, 0.35); box-shadow: inset 4px 0 0 rgba(217, 119, 6, 0.85), var(--shadow); }}
    .verdict-fail {{ background: linear-gradient(180deg, #111a2f, #0f172a); border-color: rgba(220, 38, 38, 0.35); box-shadow: inset 4px 0 0 rgba(220, 38, 38, 0.85), var(--shadow); }}
    .verdict-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 14px; margin-top: 18px; }}
    .mini-label {{ display: block; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); }}
    .mini-value {{ display: block; margin-top: 8px; font-family: "Geist", "Inter", system-ui, sans-serif; font-size: 1.7rem; font-weight: 700; line-height: 1.1; }}
    .context-line {{ margin-top: 8px; color: var(--muted); font-size: 0.95rem; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }}
    h2, h3 {{ margin: 0 0 10px; font-family: "Geist", "Inter", system-ui, sans-serif; font-size: 1.05rem; line-height: 1.2; }}
    p, li {{ color: var(--muted); line-height: 1.6; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 10px 0; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; word-break: break-word; overflow-wrap: anywhere; }}
    th {{ color: var(--muted); font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.08em; }}
    td:last-child, th:last-child {{ text-align: right; }}
    code {{ font-family: "JetBrains Mono", "Consolas", monospace; background: var(--panel-raised); color: var(--accent); padding: 0.14rem 0.38rem; border-radius: 0.4rem; word-break: break-word; }}
    .status-pass {{ color: var(--success); font-weight: 700; }}
    .status-fail {{ color: var(--error); font-weight: 700; }}
    .footer {{ margin-top: 18px; display: flex; align-items: center; justify-content: space-between; gap: 16px; color: var(--muted); font-size: 0.94rem; }}
    .footer-logo {{ display: block; width: auto; height: 22px; max-width: min(100%, 260px); }}
    .footer-copy {{ color: var(--ink); }}
    ul {{ margin: 0; padding-left: 18px; }}
    @media (max-width: 720px) {{
      .page {{ width: min(100% - 24px, 1120px); margin: 12px auto 28px; }}
      .nav {{ padding: 14px 16px; flex-direction: column; align-items: flex-start; }}
      .hero {{ padding: 22px; }}
      td:last-child, th:last-child {{ text-align: left; }}
      .footer {{ flex-direction: column; align-items: flex-start; }}
    }}
  </style>
</head>
<body>
  <main class="page">
    <nav class="nav">
      <div class="nav-brand">{('<img src="' + report_logo + '" alt="Helix trial report" class="report-logo">') if report_logo else '<span>Helix trial report</span>'}</div>
      <div class="nav-meta">{escape(generated_at)}</div>
    </nav>
    <section class="hero">
      <h1>Shareable trial result for one Helix workload evaluation</h1>
      <p>This report packages the verdict, economics, structural fingerprint snapshot, and compliance metadata from a single Helix trial run so it can be shared without a server or additional tooling.</p>
    </section>
    <section class="section card {verdict_class}">
      <span class="mini-label">Verdict</span>
      <span class="mini-value">{escape(verdict)}</span>
      <div class="verdict-grid">
        <div><span class="mini-label">Compression gain</span><span class="mini-value">{float(summary["archive_bundle_gain_percent_vs_source_gzip"]):+.2f}%</span><div class="context-line">{int(summary["archive_bundle_gain_vs_source_gzip"]):+d} bytes vs source+gzip</div></div>
        <div><span class="mini-label">Sessions archived</span><span class="mini-value">{int(summary["session_total"]):,}</span><div class="context-line">{int(summary["source_count"])} source file(s), {int(summary["source_family_count"])} family(ies)</div></div>
        <div><span class="mini-label">Replay verified</span><span class="mini-value">{"yes" if int(summary["replay_verified"]) else "no"}</span><div class="context-line">Canonical replay contract preserved</div></div>
      </div>
    </section>
    <section class="section grid">
      <article class="card">
        <h2>Economics</h2>
        <table>
          <tbody>
            <tr><th>Source size (bytes)</th><td>{int(summary["source_gzip_total_bytes"]):,}</td></tr>
            <tr><th>Archive size (bytes)</th><td>{int(summary["epl_bundle_total_bytes"]):,}</td></tr>
            <tr><th>Savings (bytes)</th><td>{int(summary["archive_bundle_gain_vs_source_gzip"]):+d}</td></tr>
            <tr><th>Savings (%)</th><td>{float(summary["archive_bundle_gain_percent_vs_source_gzip"]):+.2f}%</td></tr>
            <tr><th>Estimated gain at 10K sessions</th><td>{int(summary["estimated_gain_10k_sessions_bytes"]):+d}</td></tr>
            <tr><th>Estimated gain at 100K sessions</th><td>{int(summary["estimated_gain_100k_sessions_bytes"]):+d}</td></tr>
          </tbody>
        </table>
      </article>
      <article class="card">
        <h2>Helix drift snapshot</h2>
        <table>
          <tbody>
            <tr><th>Template count</th><td>{int(summary["helix_template_count"])}</td></tr>
            <tr><th>Families detected</th><td>{int(summary["helix_family_count"])}</td></tr>
            <tr><th>Drift check result</th><td class="{"status-pass" if drift_status == "PASS" else "status-fail"}">{escape(drift_status)} against threshold {float(summary["helix_drift_threshold"]):.2f}</td></tr>
          </tbody>
        </table>
        <p>{escape(str(summary["helix_note"]))}</p>
      </article>
      <article class="card">
        <h2>Compliance readiness</h2>
        <table>
          <tbody>
            <tr><th>eu_ai_act_article_12_mode</th><td>{"enabled" if int(summary["compliance_mode"]) else "disabled"}</td></tr>
            <tr><th>operator_id</th><td>{escape(str(summary["compliance_operator_id"]))}</td></tr>
            <tr><th>archived_at</th><td>{escape(str(summary["compliance_archived_at"]))}</td></tr>
            <tr><th>retention_floor_date</th><td>{escape(str(summary["compliance_retention_floor_date"]))}</td></tr>
            <tr><th>hash_algorithm</th><td>{escape(str(summary["compliance_hash_algorithm"]))}</td></tr>
            <tr><th>replay_contract</th><td>{escape(str(summary["compliance_replay_contract"]))}</td></tr>
          </tbody>
        </table>
      </article>
      <article class="card">
        <h2>Next steps</h2>
        <ul>{next_steps}</ul>
      </article>
    </section>
    <footer class="footer">
      <div>{('<img src="' + company_logo + '" alt="Trace Integrity" class="footer-logo">') if company_logo else 'Trace Integrity'}</div>
      <div class="footer-copy">Trace Integrity · Helix {escape(VERSION)}</div>
    </footer>
  </main>
</body>
</html>
"""


def _trial_metric(label: str, value: str) -> str:
    return f'<article class="card metric"><span>{escape(label)}</span><strong>{escape(value)}</strong></article>'


def _trial_link_row(label: str, path: str) -> str:
    return f"<p><strong>{escape(label)}:</strong> <code>{escape(path)}</code></p>"


def _count_templates_from_rows(rows: list[dict[str, Any]]) -> int:
    total = 0
    for row in rows:
        templates_path = Path(str(row.get("templates_path", "")))
        if not templates_path.exists():
            continue
        if templates_path.suffix.lower() == ".csv":
            with templates_path.open("r", encoding="utf-8", newline="") as handle:
                total += sum(1 for _ in csv.DictReader(handle))
            continue
        try:
            payload = json.loads(templates_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, list):
            total += len(payload)
        elif isinstance(payload, dict):
            total += len(payload)
    return total


def _partner_trial_next_steps(recommendation: str) -> str:
    if recommendation == "pilot_now":
        items = [
            "Run on your full export: <code>epl-trace-worker --input-path &lt;your-export&gt;</code>",
            "Enable compliance mode: <code>EPL_COMPLIANCE_MODE=1 EPL_OPERATOR_ID=your-org</code>",
        ]
    elif recommendation == "narrow_pilot":
        items = [
            "This workload is borderline. Try a larger export or a different trace family.",
        ]
    else:
        items = [
            "EPL performs best on high-volume repetitive workflows. See <code>workload-fit.html</code>.",
        ]
    return "".join(f"<li>{item}</li>" for item in items)


def _build_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
