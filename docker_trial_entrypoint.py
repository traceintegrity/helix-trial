from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from epl.traces import parse_retention_candidates_spec, run_trial_box


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run EPL trial-box inside the Docker trial image.")
    parser.add_argument("input_path", type=Path)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--window-size", type=int, default=6)
    parser.add_argument("--stream-replay-passes", type=int, default=4)
    parser.add_argument("--max-active-templates", type=int, default=8)
    parser.add_argument("--retention-candidates", default="4,8,12,16")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = args.input_path.resolve()
    output_dir = (args.output_dir or input_path.parent).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    result = run_trial_box(
        input_path=input_path,
        output_dir=output_dir,
        window_size=args.window_size,
        max_active_templates=None if args.max_active_templates == 0 else args.max_active_templates,
        stream_replay_passes=args.stream_replay_passes,
        retention_candidates=parse_retention_candidates_spec(args.retention_candidates),
    )

    stem = input_path.stem
    copied = {
        "summary_json": output_dir / f"{stem}.epl_trial_summary.json",
        "summary_md": output_dir / f"{stem}.epl_trial_summary.md",
        "metrics_json": output_dir / f"{stem}.epl_trial_metrics.json",
        "manifest_json": output_dir / f"{stem}.epl_trial_manifest.json",
    }
    shutil.copyfile(Path(str(result["summary_json_path"])), copied["summary_json"])
    shutil.copyfile(Path(str(result["summary_md_path"])), copied["summary_md"])
    shutil.copyfile(Path(str(result["metrics_json_path"])), copied["metrics_json"])
    shutil.copyfile(Path(str(result["manifest_copy_path"])), copied["manifest_json"])

    print(f"verdict={result['verdict']}")
    print(f"archive_gain_vs_source_gzip_bytes={int(result['archive_gain_vs_source_gzip_bytes']):+d}")
    print(f"replay_verified={int(result['replay_verified'])}")
    print(f"summary_json={copied['summary_json'].as_posix()}")
    print(f"summary_md={copied['summary_md'].as_posix()}")
    print(f"metrics_json={copied['metrics_json'].as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
