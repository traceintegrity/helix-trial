from collections.abc import Mapping, Sequence
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def _cumulative_average(values: Sequence[float]) -> list[float]:
    running_total = 0.0
    averages: list[float] = []

    for index, value in enumerate(values, start=1):
        running_total += value
        averages.append(running_total / index)

    return averages


def generate_plots(
    episode_metrics: Sequence[Mapping[str, object]],
    output_dir: Path,
    run_id: str,
    phase_boundary_episode: int | None = None,
) -> list[Path]:
    if not episode_metrics:
        raise ValueError("episode_metrics must not be empty")

    output_dir.mkdir(parents=True, exist_ok=True)

    episodes = [int(row["episode"]) for row in episode_metrics]
    success_values = [float(row["success"]) for row in episode_metrics]
    message_lengths = [float(row["message_length"]) for row in episode_metrics]
    vocabulary_sizes = [float(row["vocabulary_size"]) for row in episode_metrics]

    plot_specs = [
        (
            output_dir / f"{run_id}_success_rate_over_time.png",
            _cumulative_average(success_values),
            "Success Rate Over Time",
            "Cumulative Success Rate",
        ),
        (
            output_dir / f"{run_id}_average_message_length_over_time.png",
            _cumulative_average(message_lengths),
            "Average Message Length Over Time",
            "Cumulative Average Message Length",
        ),
        (
            output_dir / f"{run_id}_protocol_vocabulary_size_over_time.png",
            vocabulary_sizes,
            "Protocol Vocabulary Size Over Time",
            "Vocabulary Size",
        ),
    ]

    paths: list[Path] = []
    for path, values, title, y_label in plot_specs:
        figure, axis = plt.subplots(figsize=(8, 4.5))
        axis.plot(episodes, values, linewidth=2)
        if phase_boundary_episode is not None and episodes and phase_boundary_episode < episodes[-1]:
            axis.axvline(phase_boundary_episode + 0.5, color="#c06031", linestyle="--", linewidth=1.5)
            axis.text(
                phase_boundary_episode + 0.8,
                max(values) if values else 0.0,
                "eval starts",
                color="#c06031",
                fontsize=9,
                verticalalignment="top",
            )
        axis.set_title(title)
        axis.set_xlabel("Episode")
        axis.set_ylabel(y_label)
        axis.grid(alpha=0.2)
        figure.tight_layout()
        figure.savefig(path)
        plt.close(figure)
        paths.append(path)

    return paths
