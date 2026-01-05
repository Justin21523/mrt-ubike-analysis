from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Mapping, Optional


def write_bronze_json(
    base_dir: Path,
    *,
    source: str,
    domain: str,
    dataset: str,
    city: str,
    retrieved_at: datetime,
    request: Optional[Mapping[str, Any]],
    payload: Any,
) -> Path:
    """
    Persist raw API payload to Bronze with minimal metadata for traceability.
    """

    ts = retrieved_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = base_dir / source / domain / dataset / f"city={city}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{ts}.json"
    wrapper = {
        "retrieved_at": retrieved_at.astimezone(timezone.utc).isoformat(),
        "request": dict(request) if request else None,
        "payload": payload,
    }
    out_path.write_text(json.dumps(wrapper, ensure_ascii=False), encoding="utf-8")
    return out_path


def read_bronze_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))

