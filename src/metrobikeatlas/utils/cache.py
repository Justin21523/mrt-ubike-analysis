from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import time
from typing import Any, Optional


from metrobikeatlas.config.models import CacheSettings


class JsonFileCache:
    """
    Small file-based JSON cache for HTTP responses and derived artifacts.

    This is intentionally simple for the MVP. It can be replaced by Redis/S3 later.
    """

    def __init__(self, settings: CacheSettings) -> None:
        self._dir = settings.dir
        self._ttl = settings.ttl_seconds
        self._dir.mkdir(parents=True, exist_ok=True)

    def make_key(self, namespace: str, payload: Any) -> str:
        raw = json.dumps({"ns": namespace, "payload": payload}, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _path(self, key: str) -> Path:
        return self._dir / f"{key}.json"

    def get(self, key: str) -> Optional[Any]:
        path = self._path(key)
        if not path.exists():
            return None
        try:
            wrapper = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None

        created_at = wrapper.get("_created_at")
        if not isinstance(created_at, (int, float)):
            return None
        if self._ttl > 0 and (time.time() - float(created_at)) > self._ttl:
            return None
        return wrapper.get("payload")

    def set(self, key: str, payload: Any) -> None:
        path = self._path(key)
        wrapper = {"_created_at": time.time(), "payload": payload}
        serialized = json.dumps(wrapper, ensure_ascii=False)

        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=path.parent) as tmp:
            tmp.write(serialized)
            tmp_path = Path(tmp.name)
        tmp_path.replace(path)
