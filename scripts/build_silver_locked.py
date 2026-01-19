from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import time


def _is_pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _lock_path(repo_root: Path) -> Path:
    return repo_root / "logs" / "locks" / "build_silver.lock"


def _try_acquire(lock_file: Path) -> tuple[bool, int | None]:
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(lock_file), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        try:
            existing = int(lock_file.read_text(encoding="utf-8").strip())
        except Exception:
            existing = None
        if existing and not _is_pid_running(existing):
            try:
                lock_file.unlink()
            except Exception:
                return False, existing
            return _try_acquire(lock_file)
        return False, existing
    else:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(str(os.getpid()))
        return True, None


def _release(lock_file: Path) -> None:
    try:
        lock_file.unlink()
    except Exception:
        pass


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Run build_silver.py under a cross-process lock.")
    parser.add_argument("--wait-seconds", type=int, default=int(os.getenv("BUILD_SILVER_WAIT_SECONDS", "3600")))
    args, rest = parser.parse_known_args()

    lock_file = _lock_path(repo_root)
    wait_s = max(int(args.wait_seconds), 0)
    started = time.monotonic()

    while True:
        ok, existing_pid = _try_acquire(lock_file)
        if ok:
            break
        if wait_s <= 0:
            sys.stderr.write(f"build_silver locked by pid={existing_pid}\n")
            return 3
        if time.monotonic() - started > wait_s:
            sys.stderr.write(f"build_silver lock timeout (pid={existing_pid})\n")
            return 4
        time.sleep(2.0)

    # Run the real script under this lock, then release.
    script = repo_root / "scripts" / "build_silver.py"
    cmd = [sys.executable, str(script)] + rest
    try:
        proc = subprocess.run(cmd, cwd=str(repo_root))
        return int(proc.returncode)
    finally:
        _release(lock_file)


if __name__ == "__main__":
    raise SystemExit(main())
