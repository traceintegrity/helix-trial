import csv
import json
from collections.abc import Iterable, Mapping
from pathlib import Path


def ensure_dirs(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def write_jsonl(path: Path, rows: Iterable[Mapping[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def write_csv(path: Path, rows: Iterable[Mapping[str, object]]) -> None:
    materialized_rows = list(rows)
    if not materialized_rows:
        path.write_text("", encoding="utf-8")
        return

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(materialized_rows[0].keys()))
        writer.writeheader()
        writer.writerows(materialized_rows)
