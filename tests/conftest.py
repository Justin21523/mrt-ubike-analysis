from __future__ import annotations

import sys
from pathlib import Path


def pytest_configure() -> None:
    """
    Keep a `src/` layout while allowing `pytest` to run without requiring an editable install.
    """

    project_root = Path(__file__).resolve().parents[1]
    src_path = project_root / "src"
    sys.path.insert(0, str(src_path))

