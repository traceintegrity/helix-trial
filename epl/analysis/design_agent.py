from dataclasses import dataclass
from typing import Literal

from epl.version_history import VersionRecord


Tone = Literal["breakthrough", "caution", "early"]


@dataclass(frozen=True, slots=True)
class SignalCard:
    label: str
    value: str
    detail: str


@dataclass(frozen=True, slots=True)
class BriefCard:
    title: str
    body: str


@dataclass(frozen=True, slots=True)
class DesignBrief:
    tone: Tone
    theme_name: str
    eyebrow: str
    headline: str
    subhead: str
    status_label: str
    status_title: str
    status_body: str
    accent: str
    accent_soft: str
    hero_start: str
    hero_end: str
    hero_signals: tuple[SignalCard, ...]
    briefing_cards: tuple[BriefCard, ...]
    next_move: str


def build_design_brief(summary_row: dict[str, object], current_release: VersionRecord) -> DesignBrief:
    success_rate = float(summary_row.get("success_rate", 0.0))
    overall_net_ratio = float(summary_row.get("net_compression_ratio", 0.0))
    eval_episodes = int(summary_row.get("eval_episodes", 0))
    eval_net_ratio = float(summary_row.get("eval_net_compression_ratio", 0.0))
    loaded_entries = int(summary_row.get("loaded_memory_entries", 0))
    persisted_entries = int(summary_row.get("persisted_memory_entries", 0))
    policy_rows = int(summary_row.get("policy_candidate_rows", 0))
    eval_success_rate = float(summary_row.get("eval_success_rate", 0.0))
    active_promotion_policy = str(summary_row.get("active_promotion_policy", "utility_rule")).replace("_", " ").title()
    live_promotions = int(summary_row.get("policy_live_promotions", 0))
    intervention_rows = int(summary_row.get("policy_intervention_rows", 0))
    training_signal = str(summary_row.get("policy_training_signal", "unknown")).replace("_", " ")

    if success_rate >= 0.999 and eval_episodes > 0 and eval_success_rate >= 0.999 and eval_net_ratio < 1.0:
        tone: Tone = "breakthrough"
    elif success_rate >= 0.999 and overall_net_ratio < 1.0:
        tone = "caution"
    else:
        tone = "early"

    if tone == "breakthrough":
        theme_name = "breakthrough"
        headline = "Held-Out Compression Is Clearing The Cost Line"
        subhead = "The system is no longer only accurate and compact in aggregate. It is now producing net-positive compression on the frozen held-out split, which is the first credible sign of reusable learned structure."
        status_label = "Board Signal"
        status_title = "Progress Is Translating Into Generalization"
        status_body = "This run keeps correctness intact and beats literal transmission on the held-out split after memory cost is charged. That is the threshold where compression research starts looking like product progress."
        accent = "#8abf6a"
        accent_soft = "rgba(138, 191, 106, 0.16)"
        hero_start = "#14352f"
        hero_end = "#2a6d5d"
    elif tone == "caution":
        theme_name = "caution"
        headline = "The Codec Is Working, But Held-Out Efficiency Still Lags"
        subhead = "The system is accurate and net-positive across the full run, but the frozen eval split still costs more than it saves. The architecture is moving forward, yet the learning policy still needs to improve."
        status_label = "Board Signal"
        status_title = "Overall Progress Is Real, Generalization Is The Constraint"
        status_body = "This is a good engineering state, not a finished research state. We have a functioning codec and a credible evaluation loop, but we do not yet have evidence that held-out efficiency is consistently paying for retained memory."
        accent = "#d66b34"
        accent_soft = "rgba(214, 107, 52, 0.16)"
        hero_start = "#16302b"
        hero_end = "#3f7567"
    else:
        theme_name = "early"
        headline = "Compression Infrastructure Is In Place, But The Economics Are Still Early"
        subhead = "The system now exposes the right measurements and surfaces, but the codec is still early-stage and has not yet proven broad economic value beyond infrastructure readiness."
        status_label = "Board Signal"
        status_title = "Use This Version For Visibility, Not Victory Claims"
        status_body = "The current value is observability and discipline. This version gives the team and stakeholders a much better view of where the codec is paying off and where it is not."
        accent = "#8d5b6f"
        accent_soft = "rgba(141, 91, 111, 0.16)"
        hero_start = "#182733"
        hero_end = "#48566f"

    hero_signals = (
        SignalCard("Overall Net", f"{overall_net_ratio:.2f}", "Whole run cost after memory"),
        SignalCard(
            "Held-Out Net",
            f"{eval_net_ratio:.2f}" if eval_episodes > 0 else "off",
            "Frozen eval after training",
        ),
        SignalCard(
            "Memory Lifecycle",
            f"{loaded_entries} -> {persisted_entries}",
            "Loaded chunks to retained chunks",
        ),
        SignalCard(
            "Policy Engine",
            active_promotion_policy,
            f"{training_signal}: {live_promotions} live promotions, {intervention_rows} interventions",
        ),
    )

    briefing_cards = (
        BriefCard(
            "Why This Version Exists",
            current_release.summary,
        ),
        BriefCard(
            "Current Operating Truth",
            f"Accuracy is {success_rate * 100:.1f}% overall and {eval_success_rate * 100:.1f}% on held-out eval. The main open question is economic efficiency on the frozen split, not basic reconstruction.",
        ),
        BriefCard(
            "Board Risk",
            "The current system can look strong in aggregate while still underperforming on held-out net compression. That is the exact failure mode this surface is designed to make impossible to miss.",
        ),
        BriefCard(
            "What The Team Should Do Next",
            current_release.next_step,
        ),
    )

    return DesignBrief(
        tone=tone,
        theme_name=theme_name,
        eyebrow="Helix",
        headline=headline,
        subhead=subhead,
        status_label=status_label,
        status_title=status_title,
        status_body=status_body,
        accent=accent,
        accent_soft=accent_soft,
        hero_start=hero_start,
        hero_end=hero_end,
        hero_signals=hero_signals,
        briefing_cards=briefing_cards,
        next_move=current_release.next_step,
    )
