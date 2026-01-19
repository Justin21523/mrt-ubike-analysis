from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import uuid


@dataclass(frozen=True)
class StoredSnapshot:
    id: str
    created_at_utc: datetime
    snapshot: dict[str, object]


class BriefingSnapshotStore:
    """
    Minimal local store for briefing snapshots.

    - Stored under `logs/briefing/snapshots/<id>.json`
    - No global state; safe for localhost-only admin usage
    """

    def __init__(self, *, repo_root: Path) -> None:
        self._root = Path(repo_root)
        self._dir = self._root / "logs" / "briefing" / "snapshots"
        self._dir.mkdir(parents=True, exist_ok=True)

    @property
    def dir(self) -> Path:
        return self._dir

    def create(self, snapshot: dict[str, object]) -> StoredSnapshot:
        sid = uuid.uuid4().hex
        created = datetime.now(timezone.utc)
        payload = {
            "id": sid,
            "created_at_utc": created.isoformat(),
            "snapshot": snapshot,
        }
        path = self._dir / f"{sid}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return StoredSnapshot(id=sid, created_at_utc=created, snapshot=snapshot)

    def list(self, *, limit: int = 50) -> list[StoredSnapshot]:
        files = sorted(self._dir.glob("*.json"), key=lambda p: p.name, reverse=True)
        out: list[StoredSnapshot] = []
        for p in files[: max(int(limit), 1)]:
            try:
                obj = json.loads(p.read_text(encoding="utf-8"))
                created = datetime.fromisoformat(obj.get("created_at_utc")).astimezone(timezone.utc)
                out.append(
                    StoredSnapshot(
                        id=str(obj.get("id") or p.stem),
                        created_at_utc=created,
                        snapshot=dict(obj.get("snapshot") or {}),
                    )
                )
            except Exception:
                continue
        return out

    def get(self, snapshot_id: str) -> StoredSnapshot | None:
        p = self._dir / f"{snapshot_id}.json"
        if not p.exists():
            return None
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            created = datetime.fromisoformat(obj.get("created_at_utc")).astimezone(timezone.utc)
            return StoredSnapshot(
                id=str(obj.get("id") or snapshot_id),
                created_at_utc=created,
                snapshot=dict(obj.get("snapshot") or {}),
            )
        except Exception:
            return None

