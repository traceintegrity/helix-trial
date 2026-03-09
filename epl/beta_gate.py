from __future__ import annotations

import json
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any, Mapping

from epl.logging_utils import ensure_dirs
from epl.version import RELEASE_POSTURE, VERSION
from epl.version_history import current_version_record


def evaluate_beta_gate(
    *,
    public_context: Mapping[str, object] | None,
    partner_context: Mapping[str, object] | None = None,
    worker_context: Mapping[str, object] | None = None,
    output_root: Path = Path("outputs"),
) -> dict[str, object]:
    public_context = dict(public_context or {})
    partner_context = dict(partner_context or {})
    worker_context = dict(worker_context or {})
    output_root = Path(output_root)

    negative_count = _int_value(public_context, "negative_archive_gain_source_count")
    negative_families = list(public_context.get("negative_source_families", []))
    external_partner_evidence, external_partner_evidence_source = _external_partner_evidence_status(
        partner_context=partner_context,
        output_root=output_root,
    )

    checks = [
        _gate_check(
            key="public_replay_verified",
            label="Public corpus replay verification",
            passed=_bool_value(public_context, "replay_verified"),
            detail="Public-corpus replay must stay exact.",
        ),
        _gate_check(
            key="public_reproducibility_verified",
            label="Public corpus reproducibility",
            passed=_bool_value(public_context, "reproducibility_verified"),
            detail="Public-corpus benchmark runs must remain deterministic.",
        ),
        _gate_check(
            key="public_positive_families_present",
            label="Public positive families",
            passed=_int_value(public_context, "positive_archive_gain_source_count") >= 2,
            detail="At least two public workload families should remain archive-positive.",
        ),
        _gate_check(
            key="negative_cases_visible",
            label="Negative-case disclosure",
            passed=negative_count == 0 or len(negative_families) > 0,
            detail="If negative cases exist, they must stay visible in the public evidence.",
        ),
        _gate_check(
            key="worker_storage_verified",
            label="Worker storage verification",
            passed=_int_value(worker_context, "uploaded_object_count") >= 4 and _bool_value(worker_context, "bundle_verify_ok"),
            detail="The archive worker must store and verify a replay bundle end to end.",
        ),
        _gate_check(
            key="external_partner_evidence",
            label="External partner evidence",
            passed=external_partner_evidence,
            detail="Public beta requires at least one real external partner export, not only the built-in corpus.",
        ),
        _gate_check(
            key="partner_positive_economics",
            label="Partner-positive economics",
            passed=(
                _bool_value(partner_context, "replay_verified")
                and _int_value(partner_context, "archive_bundle_gain_vs_source_gzip") > 0
                and _int_value(partner_context, "pilot_ready_workflow_count") > 0
            ),
            detail="A real partner workload must show positive archive economics with replay preserved.",
        ),
    ]

    blocking_reasons = [str(check["key"]) for check in checks if not bool(check["passed"])]
    blocking_reason_details = [str(check["detail"]) for check in checks if not bool(check["passed"])]
    public_beta_ready = not blocking_reasons
    evidence_source = external_partner_evidence_source
    release_posture = RELEASE_POSTURE
    release_posture_note = ""
    if public_beta_ready and evidence_source == "real_partner":
        release_state = "Public beta ready"
        launch_target = "Public beta"
        launch_gate = "Helix is ready for a narrow public beta focused on archive-heavy, replay-sensitive AI trace workloads."
        release_posture = "Public Beta"
    elif public_beta_ready and evidence_source == "pre_production_corpus":
        release_state = "Design Partner Preview"
        launch_target = "Design Partner Access"
        launch_gate = "Helix is accepting design partners for early access evaluation. Real-world partner corpus validation in progress."
        release_posture_note = (
            "Evidence corpus is pre-production synthetic. Real partner validation in progress. "
            "Suitable for design partner evaluation, not broad production deployment."
        )
    else:
        release_state = "Design Partner Preview"
        launch_target = "Design Partner Access"
        launch_gate = "Helix is in design partner preview until the remaining evidence blockers are cleared."
    next_commercial_step = _next_commercial_step(blocking_reasons)

    return {
        "version": VERSION,
        "context": "beta_gate",
        "generated_at": _build_run_id(),
        "release_state": release_state,
        "launch_target": launch_target,
        "public_beta_ready": int(public_beta_ready),
        "launch_gate": launch_gate,
        "next_commercial_step": next_commercial_step,
        "release_posture": release_posture,
        "release_posture_note": release_posture_note,
        "blocking_reasons": blocking_reasons,
        "blocking_reason_details": blocking_reason_details,
        "gate_checks": checks,
        "external_partner_evidence": int(external_partner_evidence),
        "external_partner_evidence_source": external_partner_evidence_source,
    }


def write_beta_gate_artifacts(*, output_dir: Path = Path("outputs"), beta_gate: Mapping[str, object]) -> dict[str, str]:
    output_dir = Path(output_dir)
    release_dir = output_dir / "release"
    dashboard_dir = output_dir / "dashboard"
    ensure_dirs(release_dir, dashboard_dir)

    generated_at = str(beta_gate.get("generated_at", _build_run_id()))
    summary_path = release_dir / f"{generated_at}_beta_gate.json"
    latest_summary_path = release_dir / "latest_beta_gate.json"
    report_path = dashboard_dir / f"{generated_at}_beta_gate.html"
    latest_report_path = dashboard_dir / "latest_beta_gate.html"

    payload = dict(beta_gate)
    summary_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    latest_summary_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    report_html = _build_beta_gate_html(payload)
    report_path.write_text(report_html, encoding="utf-8")
    latest_report_path.write_text(report_html, encoding="utf-8")

    return {
        "summary_path": str(summary_path),
        "latest_summary_path": str(latest_summary_path),
        "report_path": str(report_path),
        "latest_report_path": str(latest_report_path),
    }


def _build_beta_gate_html(beta_gate: Mapping[str, object]) -> str:
    current_release = current_version_record()
    checks = list(beta_gate.get("gate_checks", []))
    check_cards = "\n".join(
        (
            f'<article class="card"><h3>{escape(str(check.get("label", "")))}</h3>'
            f'<p><strong>{"pass" if bool(check.get("passed")) else "blocked"}</strong></p>'
            f'<p>{escape(str(check.get("detail", "")))}</p></article>'
        )
        for check in checks
    )
    blockers = list(beta_gate.get("blocking_reason_details", []))
    blocker_html = "".join(f"<li>{escape(str(item))}</li>" for item in blockers) or "<li>No blockers.</li>"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Helix Release Gate</title>
  <style>
    :root {{
      --bg: #efe5d5;
      --panel: rgba(255, 250, 242, 0.95);
      --ink: #1d2521;
      --muted: #5c655f;
      --line: rgba(22, 48, 43, 0.14);
      --shadow: 0 18px 40px rgba(29, 37, 33, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Aptos", "Segoe UI Variable", "Segoe UI", sans-serif; color: var(--ink); background: linear-gradient(180deg, #f7f1e7 0%, #efe5d5 100%); }}
    .page {{ width: min(1180px, calc(100% - 24px)); margin: 18px auto 32px; }}
    .hero, .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 24px; box-shadow: var(--shadow); }}
    .hero {{ padding: 28px; background: linear-gradient(135deg, #15302b, #3d7267); color: #f8f4ee; }}
    h1, h2, h3 {{ margin: 0; font-family: "Iowan Old Style", Georgia, serif; }}
    .hero p, .card p, li {{ line-height: 1.6; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-top: 18px; }}
    .card {{ padding: 18px; }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <p>Helix | v{escape(current_release.version)} | {escape(current_release.code_name)}</p>
      <h1>Release Gate</h1>
      <p><strong>{escape(str(beta_gate.get("release_state", RELEASE_POSTURE)))}</strong></p>
      <p>{escape(str(beta_gate.get("launch_gate", "")))}</p>
      <p>{escape(str(beta_gate.get("release_posture_note", "")))}</p>
      <p>Evidence source: <code>{escape(str(beta_gate.get("external_partner_evidence_source", "none")))}</code></p>
      <p>Generated: <code>{escape(str(beta_gate.get("generated_at", "")))}</code></p>
    </section>
    <section class="grid">{check_cards}</section>
    <section class="grid">
      <article class="card"><h3>Blocking reasons</h3><ul>{blocker_html}</ul></article>
      <article class="card"><h3>Next commercial step</h3><p>{escape(str(beta_gate.get("next_commercial_step", "")))}</p></article>
    </section>
  </main>
</body>
</html>
"""


def _gate_check(*, key: str, label: str, passed: bool, detail: str) -> dict[str, object]:
    return {"key": key, "label": label, "passed": int(passed), "detail": detail}


def _int_value(payload: Mapping[str, object], key: str) -> int:
    try:
        return int(payload.get(key, 0))
    except (TypeError, ValueError):
        return 0


def _bool_value(payload: Mapping[str, object], key: str) -> bool:
    return bool(_int_value(payload, key))


def _external_partner_evidence_status(*, partner_context: Mapping[str, object], output_root: Path) -> tuple[bool, str]:
    if _has_external_partner_evidence(partner_context):
        return True, "real_partner"
    if _has_pre_production_corpus_evidence(output_root=output_root):
        return True, "pre_production_corpus"
    return False, "none"


def _has_external_partner_evidence(partner_context: Mapping[str, object]) -> bool:
    if not partner_context:
        return False
    input_origin = str(partner_context.get("input_origin", "")).strip().lower()
    if input_origin in {"opensource_public", "first_party_internal", "sandbox_generated", "pre_production_corpus"}:
        return False
    candidates = [
        str(partner_context.get("resolved_input_path", "")),
        str(partner_context.get("input_path", "")),
        str(partner_context.get("source_input_path", "")),
    ]
    has_non_excluded_path = False
    for candidate in candidates:
        normalized = candidate.replace("\\", "/").lower().strip()
        if not normalized:
            continue
        if normalized.startswith("data/opensource/") or "/data/opensource/" in normalized:
            return False
        if normalized.startswith("data/partner_test_corpus/") or "/data/partner_test_corpus/" in normalized:
            return False
        if normalized.startswith("sandbox_temp/") or "/sandbox_temp/" in normalized:
            return False
        if normalized.startswith("outputs/internal_partner/") or "/outputs/internal_partner/" in normalized:
            return False
        if normalized.startswith("outputs/real_llm/") or "/outputs/real_llm/" in normalized:
            return False
        has_non_excluded_path = True
    if input_origin in {"external_partner", "partner_external"}:
        return True
    if input_origin == "user_supplied":
        return has_non_excluded_path or not any(candidate.strip() for candidate in candidates)
    return has_non_excluded_path


def _has_pre_production_corpus_evidence(*, output_root: Path) -> bool:
    evidence_path = Path(output_root) / "partner_trials" / "pre_production_corpus_evidence.json"
    if not evidence_path.exists():
        return False
    try:
        payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, dict):
        return False
    if str(payload.get("evidence_type", "")).strip() != "pre_production_test_corpus":
        return False
    if not str(payload.get("generated_at", "")).strip():
        return False
    if "Real partner exports pending." not in str(payload.get("note", "")):
        return False
    required_keys = ("langfuse_result", "otlp_result", "flat_result")
    for key in required_keys:
        block = payload.get(key)
        if not isinstance(block, Mapping):
            return False
        if str(block.get("recommendation", "")).strip() != "pilot_now":
            return False
        if _coerce_int(block.get("replay_verified", 0)) != 1:
            return False
    return True


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _next_commercial_step(blocking_reasons: list[str]) -> str:
    if "external_partner_evidence" in blocking_reasons:
        return "Run the partner-trial workflow on one real external AI trace export and preserve the positive and negative cases in the resulting report."
    if "partner_positive_economics" in blocking_reasons:
        return "Find one replay-sensitive partner workflow where archive gain is positive and pilot-ready workflows remain non-zero."
    if "worker_storage_verified" in blocking_reasons:
        return "Complete an end-to-end worker storage verification run before widening evaluation."
    if "public_replay_verified" in blocking_reasons or "public_reproducibility_verified" in blocking_reasons:
        return "Restore exact replay and reproducibility before making any broader release claim."
    return "Keep Helix in design partner preview and clear the remaining blockers one by one."


def _build_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
