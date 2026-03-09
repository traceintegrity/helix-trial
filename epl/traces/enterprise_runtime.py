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
