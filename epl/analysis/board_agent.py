from __future__ import annotations

from dataclasses import dataclass
from html import escape
from typing import Mapping

from epl.version_history import VersionRecord


@dataclass(frozen=True, slots=True)
class BoardLetter:
    title: str
    paragraph_one: str
    paragraph_two: str
    launch_gate: str
    next_commercial_step: str


def build_board_letter(
    *,
    current_release: VersionRecord,
    public_context: Mapping[str, object] | None,
    partner_context: Mapping[str, object] | None,
    internal_context: Mapping[str, object] | None = None,
    launch_gate: str,
    next_commercial_step: str,
) -> BoardLetter:
    public_source_count = int(public_context.get("source_count", 0)) if public_context is not None else 0
    public_family_count = int(public_context.get("source_family_count", 0)) if public_context is not None else 0
    public_positive = int(public_context.get("positive_archive_gain_source_count", 0)) if public_context is not None else 0
    public_negative = int(public_context.get("negative_archive_gain_source_count", 0)) if public_context is not None else 0
    public_replay = bool(int(public_context.get("replay_verified", 0))) if public_context is not None else False
    public_repro = bool(int(public_context.get("reproducibility_verified", 0))) if public_context is not None else False

    public_truth = (
        f"The public-corpus truth is currently {public_source_count} source file{'s' if public_source_count != 1 else ''} "
        f"across {public_family_count} workload famil{'ies' if public_family_count != 1 else 'y'}, "
        f"with {public_positive} archive-positive famil{'ies' if public_positive != 1 else 'y'} and "
        f"{public_negative} negative case{'s' if public_negative != 1 else ''}. "
        f"Replay verification is {'passing' if public_replay else 'not yet passing'}, and reproducibility is "
        f"{'passing' if public_repro else 'not yet passing'}."
    )

    if partner_context is None:
        partner_truth = (
            "We do not yet have a partner-trial artifact, so the board should treat the current product posture as beta prep rather than launch proof. "
            "The live server and docs are ready for evaluation, but the commercial claim still depends on partner data."
        )
    else:
        recommendation = str(partner_context.get("recommendation", "unknown"))
        archive_gain = int(partner_context.get("archive_bundle_gain_vs_source_gzip", 0))
        source_count = int(partner_context.get("source_count", 0))
        family_count = int(partner_context.get("source_family_count", 0))
        pilot_ready = int(partner_context.get("pilot_ready_workflow_count", 0))
        input_origin = str(partner_context.get("input_origin", "unknown")).strip().lower()
        if input_origin == "user_supplied":
            partner_truth = (
                f"The latest external partner-trial truth is {recommendation} on {source_count} source file{'s' if source_count != 1 else ''} "
                f"across {family_count} workload famil{'ies' if family_count != 1 else 'y'}. "
                f"The partner-trial archive result is {archive_gain:+d} bytes versus source plus gzip, "
                f"with {pilot_ready} pilot-ready workflow{'s' if pilot_ready != 1 else ''}."
            )
        elif input_origin == "first_party_internal":
            partner_truth = (
                f"The latest saved trial is first-party internal evidence, not external partner proof. "
                f"It currently reads {recommendation} on {source_count} source file{'s' if source_count != 1 else ''} "
                f"across {family_count} workload famil{'ies' if family_count != 1 else 'y'}, with "
                f"{archive_gain:+d} bytes versus source plus gzip and {pilot_ready} pilot-ready "
                f"workflow{'s' if pilot_ready != 1 else ''}."
            )
        elif input_origin == "opensource_public":
            partner_truth = (
                f"The latest saved trial is based on external public data, not a private partner export. "
                f"It currently reads {recommendation} on {source_count} source file{'s' if source_count != 1 else ''} "
                f"across {family_count} workload famil{'ies' if family_count != 1 else 'y'}, with "
                f"{archive_gain:+d} bytes versus source plus gzip and {pilot_ready} pilot-ready "
                f"workflow{'s' if pilot_ready != 1 else ''}."
            )
        else:
            partner_truth = (
                f"The latest saved trial is {recommendation} on {source_count} source file{'s' if source_count != 1 else ''} "
                f"across {family_count} workload famil{'ies' if family_count != 1 else 'y'}. "
                f"Its archive result is {archive_gain:+d} bytes versus source plus gzip, "
                f"with {pilot_ready} pilot-ready workflow{'s' if pilot_ready != 1 else ''}. "
                "Its provenance is not yet strong enough to count as external partner proof."
            )

    internal_truth = ""
    if internal_context is not None:
        internal_truth = (
            f" We also now have first-party dogfood evidence: the internal partner app generated "
            f"{int(internal_context.get('generated_batch_count', 0))} batches and "
            f"{int(internal_context.get('generated_session_count', 0))} sessions, then produced "
            f"{str(internal_context.get('recommendation', 'not run yet'))} with "
            f"{int(internal_context.get('archive_bundle_gain_vs_source_gzip', 0)):+d} bytes versus source plus gzip. "
            "That helps us pressure-test the product on our own workload, but it does not clear the external-partner gate."
        )

    return BoardLetter(
        title=f"Board Letter | v{current_release.version}",
        paragraph_one=(
            f"This release is {current_release.version}, {current_release.code_name}. "
            f"In plain English, the product work is now focused on design-partner beta proof rather than broad launch language. "
            f"{public_truth}"
        ),
        paragraph_two=(
            f"{partner_truth}{internal_truth} The launch gate is: {launch_gate} "
            f"The next commercial step is: {next_commercial_step}"
        ),
        launch_gate=launch_gate,
        next_commercial_step=next_commercial_step,
    )


def build_board_letter_html(letter: BoardLetter, *, current_release: VersionRecord) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="10">
  <title>{escape(letter.title)}</title>
  <style>
    :root {{
      --bg: #f4efe6;
      --panel: rgba(255, 250, 242, 0.94);
      --ink: #1d2521;
      --muted: #5c655f;
      --line: rgba(22, 48, 43, 0.14);
      --accent: #c36135;
      --shadow: 0 18px 40px rgba(29, 37, 33, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Aptos", "Segoe UI Variable", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(239, 201, 167, 0.42), transparent 30%),
        radial-gradient(circle at top right, rgba(47, 111, 98, 0.18), transparent 26%),
        linear-gradient(180deg, #f7f1e7 0%, #efe5d5 100%);
    }}
    .page {{ width: min(1080px, calc(100% - 24px)); margin: 18px auto 32px; }}
    .hero, .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: var(--shadow);
    }}
    .hero {{
      padding: 28px;
      background: linear-gradient(135deg, #15302b, #3d7267);
      color: #f8f4ee;
    }}
    .hero p {{ color: rgba(248, 244, 238, 0.88); line-height: 1.7; max-width: 860px; }}
    .eyebrow {{ text-transform: uppercase; letter-spacing: 0.14em; font-size: 0.76rem; color: var(--muted); }}
    .hero .eyebrow {{ color: rgba(248, 244, 238, 0.72); }}
    h1, h2, h3 {{ font-family: "Iowan Old Style", Georgia, serif; margin: 0; }}
    .section {{ margin-top: 20px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 260px), 1fr)); gap: 14px; }}
    .card {{ padding: 18px; }}
    .card p {{ color: var(--muted); line-height: 1.6; margin: 10px 0 0; }}
    a {{ color: var(--accent); text-decoration: none; font-weight: 700; }}
    @media (max-width: 760px) {{
      .page {{ width: min(100% - 14px, 1080px); margin: 10px auto 24px; }}
      .hero, .card {{ border-radius: 20px; padding: 16px; }}
    }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <p class="eyebrow">Helix | v{escape(current_release.version)} | {escape(current_release.code_name)}</p>
      <h1>{escape(letter.title)}</h1>
      <p>{escape(letter.paragraph_one)}</p>
      <p>{escape(letter.paragraph_two)}</p>
      <p><a href="latest_dashboard.html">Return To Program Dashboard</a></p>
    </section>
    <section class="section">
      <div class="grid">
        <article class="card">
          <h3>Launch Gate</h3>
          <p>{escape(letter.launch_gate)}</p>
        </article>
        <article class="card">
          <h3>Next Commercial Step</h3>
          <p>{escape(letter.next_commercial_step)}</p>
        </article>
      </div>
    </section>
  </main>
</body>
</html>
"""


def build_board_letter_markdown(letter: BoardLetter, *, current_release: VersionRecord) -> str:
    return (
        f"# {letter.title}\n\n"
        f"*Helix | v{current_release.version} | {current_release.code_name}*\n\n"
        f"{letter.paragraph_one}\n\n"
        f"{letter.paragraph_two}\n\n"
        f"## Launch Gate\n\n{letter.launch_gate}\n\n"
        f"## Next Commercial Step\n\n{letter.next_commercial_step}\n"
    )
