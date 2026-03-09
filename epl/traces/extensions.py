from __future__ import annotations

import importlib
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class ArchiveManifestContext:
    run_id: str
    output_dir: Path
    source_rows: list[dict[str, object]]
    window_size: int
    max_active_templates: int | None
    stream_replay_passes: int


@dataclass(frozen=True, slots=True)
class WorkerHookContext:
    output_dir: Path
    internal_dir: Path
    input_path: Path
    run_id: str
    archive_result: dict[str, Any]


class EnterpriseExtensions(Protocol):
    def enrich_archive_manifest(
        self,
        *,
        archive_manifest: dict[str, object],
        context: ArchiveManifestContext,
    ) -> dict[str, object]:
        ...

    def post_archive_worker(self, *, context: WorkerHookContext) -> dict[str, Any]:
        ...


class NoOpEnterpriseExtensions:
    def enrich_archive_manifest(
        self,
        *,
        archive_manifest: dict[str, object],
        context: ArchiveManifestContext,
    ) -> dict[str, object]:
        return archive_manifest

    def post_archive_worker(self, *, context: WorkerHookContext) -> dict[str, Any]:
        return {
            "template_signature_count": 0,
            "template_signature_db_path": str(context.internal_dir / "epl_internal.db"),
        }


@lru_cache(maxsize=1)
def get_enterprise_extensions() -> EnterpriseExtensions:
    try:
        module = importlib.import_module("ee.extensions")
    except ImportError:
        return NoOpEnterpriseExtensions()
    factory = getattr(module, "build_enterprise_extensions", None)
    if callable(factory):
        return factory()
    return NoOpEnterpriseExtensions()


__all__ = [
    "ArchiveManifestContext",
    "EnterpriseExtensions",
    "NoOpEnterpriseExtensions",
    "WorkerHookContext",
    "get_enterprise_extensions",
]
