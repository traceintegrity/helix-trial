"""Stub for the enterprise edition build process.

This module is not used by the open-source trial.  All enterprise
feature gating in the trial codebase goes through
``epl.traces.extensions`` instead.  The file is kept so that the
package layout matches the enterprise edition, which imports this
module during its own build.
"""
from __future__ import annotations

import importlib
from typing import Any


def load_enterprise_module(module_name: str) -> Any:
    try:
        return importlib.import_module(module_name)
    except ImportError as exc:  # pragma: no cover - exercised via wrappers
        raise RuntimeError(
            "Enterprise edition modules are not installed. This feature is only available in /ee builds."
        ) from exc


__all__ = [
    "load_enterprise_module",
]
