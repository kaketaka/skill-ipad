from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from .config import ROOT_DIR
from .service import get_dashboard


def export_static_site(output_dir: str | Path = "docs") -> dict[str, Any]:
    destination = Path(output_dir)
    if not destination.is_absolute():
        destination = ROOT_DIR / destination
    destination.mkdir(parents=True, exist_ok=True)

    static_dir = ROOT_DIR / "static"
    for item in static_dir.iterdir():
        if item.is_file():
            shutil.copy2(item, destination / item.name)

    state = get_dashboard()
    state["mode"] = "static"
    (destination / "state.json").write_text(
        json.dumps(state, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return {"ok": True, "output": str(destination), "state_file": str(destination / "state.json")}
