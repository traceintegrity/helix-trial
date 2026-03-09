from __future__ import annotations

import argparse
from pathlib import Path

from epl.traces.trial_box import run_trial_box


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Helix Trial - open source local trace evaluation")
    parser.add_argument("input_path", type=Path)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--window-size", type=int, default=6)
    parser.add_argument("--stream-replay-passes", type=int, default=4)
    parser.add_argument("--max-active-templates", type=int, default=8)
    args = parser.parse_args(argv)

    input_path = args.input_path.resolve()
    output_dir = (args.output_dir.resolve() if args.output_dir is not None else (Path.cwd() / "helix_trial_output")).resolve()
    result = run_trial_box(
        input_path=input_path,
        output_dir=output_dir,
        window_size=args.window_size,
        max_active_templates=None if args.max_active_templates == 0 else args.max_active_templates,
        stream_replay_passes=args.stream_replay_passes,
    )

    print("\nTrial complete\n")
    print(f"Verdict: {result['verdict']}")
    print(f"Archive gain vs source+gzip: {float(result['archive_gain_vs_source_gzip_percent']) * 100:.2f}%")
    print(f"Replay verified: {'yes' if int(result['replay_verified']) else 'no'}")
    print(f"Report: {result['latest_summary_html']}")
    return 0


__all__ = ["main"]
