from __future__ import annotations

import sys
from pathlib import Path

# Allow running scripts without requiring an editable install (`pip install -e .`).
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

import argparse
from datetime import datetime, timedelta, timezone
import logging
import shutil
import tarfile

from metrobikeatlas.config.loader import load_config
from metrobikeatlas.utils.logging import configure_logging


logger = logging.getLogger(__name__)


def _parse_ts_from_name(name: str) -> datetime | None:
    # Bronze files are named like: 20260119T095200Z.json
    stem = Path(name).stem
    try:
        dt = datetime.strptime(stem, "%Y%m%dT%H%M%SZ")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _archive_files(
    *,
    files: list[Path],
    out_tar_gz: Path,
    base_dir: Path,
    delete_after: bool,
) -> int:
    if not files:
        return 0
    out_tar_gz.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(out_tar_gz, mode="w:gz") as tf:
        for p in files:
            try:
                rel = p.relative_to(base_dir)
            except Exception:
                rel = p.name
            tf.add(str(p), arcname=str(rel))
    if delete_after:
        deleted = 0
        for p in files:
            try:
                p.unlink()
                deleted += 1
            except Exception:
                continue
        return deleted
    return 0


def _dir_size_bytes(root: Path) -> int:
    if not root.exists():
        return 0
    total = 0
    for p in root.rglob("*"):
        try:
            if p.is_file():
                total += int(p.stat().st_size)
        except Exception:
            continue
    return int(total)


def _disk_free_bytes(path: Path) -> int:
    try:
        usage = shutil.disk_usage(str(path))
        return int(usage.free)
    except Exception:
        return 0


def _prune_archives(
    *,
    archive_dir: Path,
    min_free_disk_bytes: int | None,
    max_archive_bytes: int | None,
) -> int:
    if not archive_dir.exists():
        return 0

    min_free = int(min_free_disk_bytes or 0)
    max_bytes = int(max_archive_bytes or 0)
    if min_free <= 0 and max_bytes <= 0:
        return 0

    deleted = 0
    archives = []
    for p in archive_dir.rglob("*.tar.gz"):
        try:
            st = p.stat()
            archives.append((float(st.st_mtime), int(st.st_size), p))
        except Exception:
            continue
    archives.sort(key=lambda t: t[0])  # oldest first

    while archives:
        free_ok = True
        size_ok = True
        if min_free > 0:
            free_ok = _disk_free_bytes(archive_dir) >= min_free
        if max_bytes > 0:
            size_ok = _dir_size_bytes(archive_dir) <= max_bytes
        if free_ok and size_ok:
            break

        _, _, p = archives.pop(0)
        try:
            p.unlink()
            deleted += 1
            logger.warning("Pruned archive due to limits: %s", p)
        except Exception:
            continue

    return deleted


def main() -> None:
    p = argparse.ArgumentParser(description="Archive old Bronze JSON files into tar.gz bundles (optional long-run ops).")
    p.add_argument("--bronze-dir", default="data/bronze")
    p.add_argument("--archive-dir", default="data/archive/bronze")
    p.add_argument("--older-than-days", type=int, default=7)
    p.add_argument(
        "--datasets",
        default="tdx/bike/availability",
        help="Comma-separated dataset roots under bronze-dir (e.g. tdx/bike/availability,tdx/bike/stations).",
    )
    p.add_argument("--delete-after-archive", action="store_true", help="Delete archived JSON files after bundling.")
    p.add_argument(
        "--min-free-disk-bytes",
        type=int,
        default=0,
        help="If set, prune oldest archives until free disk is above this threshold.",
    )
    p.add_argument(
        "--max-archive-bytes",
        type=int,
        default=0,
        help="If set, prune oldest archives until total archive size is below this threshold.",
    )
    args = p.parse_args()

    config = load_config()
    configure_logging(config.logging)

    bronze_dir = Path(args.bronze_dir)
    archive_dir = Path(args.archive_dir)

    older_than = datetime.now(timezone.utc) - timedelta(days=max(int(args.older_than_days), 0))
    dataset_roots = [d.strip().strip("/") for d in str(args.datasets).split(",") if d.strip()]
    if not dataset_roots:
        raise ValueError("No datasets specified")

    total_candidates = 0
    total_deleted = 0
    total_archives = 0
    total_pruned = 0

    for root in dataset_roots:
        ds_dir = bronze_dir / root
        if not ds_dir.exists():
            logger.warning("Dataset dir not found; skipping: %s", ds_dir)
            continue

        # Group by city partition if present; otherwise archive at dataset root.
        city_dirs = [p for p in ds_dir.glob("city=*") if p.is_dir()]
        partitions = city_dirs if city_dirs else [ds_dir]

        for part_dir in partitions:
            files = sorted(part_dir.glob("*.json"))
            old_files: list[Path] = []
            for f in files:
                ts = _parse_ts_from_name(f.name)
                if ts is None:
                    continue
                if ts < older_than:
                    old_files.append(f)
            if not old_files:
                continue

            total_candidates += len(old_files)
            day_groups: dict[str, list[Path]] = {}
            for f in old_files:
                ts = _parse_ts_from_name(f.name)
                if ts is None:
                    continue
                key = ts.strftime("%Y-%m-%d")
                day_groups.setdefault(key, []).append(f)

            for day, group in sorted(day_groups.items()):
                rel_part = part_dir.relative_to(bronze_dir)
                out = archive_dir / rel_part / f"{day}.tar.gz"
                deleted = _archive_files(files=group, out_tar_gz=out, base_dir=bronze_dir, delete_after=bool(args.delete_after_archive))
                total_archives += 1
                total_deleted += deleted
                logger.info("Archived %s files to %s (deleted=%s)", len(group), out, deleted)

    total_pruned = _prune_archives(
        archive_dir=archive_dir,
        min_free_disk_bytes=(int(args.min_free_disk_bytes) or None),
        max_archive_bytes=(int(args.max_archive_bytes) or None),
    )

    logger.info(
        "done: candidates=%s archives=%s deleted=%s pruned=%s (delete_after_archive=%s)",
        total_candidates,
        total_archives,
        total_deleted,
        int(total_pruned),
        bool(args.delete_after_archive),
    )


if __name__ == "__main__":
    main()
