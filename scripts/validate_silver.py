from __future__ import annotations

import sys
from pathlib import Path

# Allow running scripts without requiring an editable install (`pip install -e .`).
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

import argparse
import logging

from metrobikeatlas.config.loader import load_config
from metrobikeatlas.quality.silver import validate_silver_dir
from metrobikeatlas.utils.logging import configure_logging


logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate Silver tables (schema + basic sanity checks).")
    parser.add_argument("--config", default=None, help="Config JSON path.")
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any errors are found.")
    args = parser.parse_args()

    config = load_config(args.config)
    configure_logging(config.logging)

    silver_dir = Path(args.silver_dir)
    issues = validate_silver_dir(silver_dir, strict=args.strict)
    errors = sum(1 for i in issues if i.level == "error")
    warnings = sum(1 for i in issues if i.level == "warning")
    logger.info("Silver validation done. errors=%s warnings=%s", errors, warnings)


if __name__ == "__main__":
    main()
