from __future__ import annotations

# We use timezone-aware UTC timestamps in Bronze file names and metadata for consistent ordering.
from datetime import datetime, timezone
# `json` is used to serialize the Bronze wrapper payload to a human-readable on-disk format.
import json
# `Path` provides safe, cross-platform filesystem path operations (no manual string joins).
from pathlib import Path
# Typing helpers make the Bronze wrapper schema explicit while still allowing arbitrary raw payloads.
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

    # Convert retrieval time to a stable UTC timestamp string for file naming and easy sorting.
    ts = retrieved_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    # Partition the Bronze lake by source/domain/dataset and city so downstream reads can prune quickly.
    out_dir = base_dir / source / domain / dataset / f"city={city}"
    # Ensure the directory exists before writing the file (safe for first-run and cron jobs).
    out_dir.mkdir(parents=True, exist_ok=True)

    # File name uses the UTC timestamp so newer files naturally sort after older files.
    out_path = out_dir / f"{ts}.json"
    # Wrap the raw payload with minimal metadata so future rebuilds can reproduce and audit the request.
    wrapper = {
        # Store ISO-8601 UTC time for machine parsing and human readability.
        "retrieved_at": retrieved_at.astimezone(timezone.utc).isoformat(),
        # Store request metadata (path/params) so the payload is traceable back to an API call.
        "request": dict(request) if request else None,
        # Store raw payload "as-is" to preserve the source of truth for later normalization steps.
        "payload": payload,
    }
    # Serialize with `ensure_ascii=False` so Chinese station names remain readable (no \\u escapes).
    out_path.write_text(json.dumps(wrapper, ensure_ascii=False), encoding="utf-8")
    # Return the path so callers can log/print what was written (useful in pipelines).
    return out_path


def read_bronze_json(path: Path) -> dict[str, Any]:
    # Read the Bronze wrapper back into memory; downstream code can access `["payload"]` for raw records.
    return json.loads(path.read_text(encoding="utf-8"))
