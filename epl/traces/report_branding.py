from __future__ import annotations

import base64
from functools import lru_cache
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BRANDING_DIR = ROOT / "branding"


@lru_cache(maxsize=None)
def load_brand_asset_data_uri(file_name: str) -> str:
    asset_path = BRANDING_DIR / file_name
    if not asset_path.exists():
        return ""
    encoded = base64.b64encode(asset_path.read_bytes()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


__all__ = ["load_brand_asset_data_uri"]
