from __future__ import annotations

import os
from pathlib import Path


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def project_root() -> Path:
    # assume src/ is one level below root
    return Path(__file__).resolve().parents[2]


def data_dir(*parts: str) -> Path:
    return project_root() / "data" / Path(*parts)


def safe_filename(name: str) -> str:
    bad = '<>:"/\\|?*'
    out = "".join("_" if c in bad else c for c in name)
    return out.strip()
