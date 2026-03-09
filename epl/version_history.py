from dataclasses import dataclass

from epl.version import VERSION


@dataclass(frozen=True, slots=True)
class VersionRecord:
    version: str
    code_name: str
    released_on: str
    focus: str
    summary: str
    achievements: tuple[str, ...]
    next_step: str


VERSION_HISTORY: tuple[VersionRecord, ...] = (
    VersionRecord(
        version=VERSION,
        code_name="Helix community trial",
        released_on="2026-03-09",
        focus="Provide a clean downloadable Helix evaluation package for engineers.",
        summary="This bundle contains the open source Helix trial CLI, sample trace inputs, and the self-contained replay/economics report surface.",
        achievements=(
            "Open source Helix trial package for local workload evaluation.",
            "Self-contained HTML report generation.",
            "Top-level archive bundle and manifest outputs for review.",
        ),
        next_step="Run the trial on a real trace export and review the Helix trial report.",
    ),
)


def current_version_record() -> VersionRecord:
    return VERSION_HISTORY[0]
