from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from typing import Optional
import uuid


@dataclass
class Job:
    id: str
    kind: str
    created_at_utc: datetime
    started_at_utc: Optional[datetime]
    finished_at_utc: Optional[datetime]
    pid: Optional[int]
    returncode: Optional[int]
    log_path: Path
    cmd: list[str]
    persisted: bool = False
    _proc: Optional[subprocess.Popen] = None

    @property
    def status(self) -> str:
        if self._proc is not None:
            rc = self._proc.poll()
            if rc is None:
                return "running"
            if self.finished_at_utc is None:
                self.finished_at_utc = datetime.now(timezone.utc)
            self.returncode = rc
            return "succeeded" if rc == 0 else "failed"

        # Process handle not available (e.g. server restarted).
        if self.pid is None:
            return "unknown"
        if _is_pid_running(self.pid):
            return "running"
        if self.returncode is None:
            return "unknown"
        return "succeeded" if self.returncode == 0 else "failed"


def _is_pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _tail_lines(path: Path, *, max_lines: int = 30, max_bytes: int = 128_000) -> list[str]:
    if max_lines <= 0:
        return []
    if not path.exists():
        return []
    data = path.read_bytes()
    if len(data) > max_bytes:
        data = data[-max_bytes:]
    text = data.decode("utf-8", errors="replace")
    lines = [ln.rstrip("\n") for ln in text.splitlines()]
    return lines[-max_lines:]


def _parse_mba_events(lines: list[str]) -> dict[str, object] | None:
    """
    Extract the latest MBA_EVENT JSON payload from log tail lines.
    """

    last: dict[str, object] | None = None
    for ln in lines:
        if not ln.startswith("MBA_EVENT "):
            continue
        raw = ln[len("MBA_EVENT ") :].strip()
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        if isinstance(obj, dict) and obj.get("type") == "mba_event":
            last = obj
    return last


def parse_mba_event_timeline(path: Path, *, max_bytes: int = 512_000, max_events: int = 500) -> list[dict[str, object]]:
    """
    Parse MBA_EVENT JSON lines from a log file.

    We cap reads for safety in long-running jobs.
    """

    if not path.exists():
        return []
    try:
        data = path.read_bytes()
    except Exception:
        return []
    if len(data) > max_bytes:
        data = data[-max_bytes:]
    text = data.decode("utf-8", errors="replace")
    events: list[dict[str, object]] = []
    for ln in text.splitlines():
        if not ln.startswith("MBA_EVENT "):
            continue
        raw = ln[len("MBA_EVENT ") :].strip()
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        if obj.get("type") != "mba_event":
            continue
        events.append(obj)
        if len(events) >= max(int(max_events), 1):
            break
    return events


def _infer_build_silver_progress(lines: list[str]) -> tuple[str, int]:
    """
    Best-effort progress inference from build_silver logs.

    We intentionally keep this heuristic simple (no tight coupling to script internals).
    """

    text = "\n".join(lines)
    stages = [
        ("metro_stations", "Wrote data/silver/metro_stations.csv"),
        ("bike_stations", "Wrote data/silver/bike_stations.csv"),
        ("bike_timeseries", "Wrote data/silver/bike_timeseries.csv"),
        ("links", "Wrote data/silver/metro_bike_links.csv"),
    ]
    done = 0
    last_stage = "starting"
    for stage, marker in stages:
        if marker in text:
            done += 1
            last_stage = stage
    if done == len(stages):
        return "done", 100
    # 0..100 mapped to 5 buckets (starting + 4 outputs)
    pct = int(round((done / max(len(stages), 1)) * 100))
    return last_stage, max(min(pct, 99), 0)


def apply_build_silver_overrides(args: list[str], overrides: dict[str, object]) -> list[str]:
    """
    Apply common build_silver.py overrides to a list of CLI args.

    This is intentionally conservative: it handles a small set of stable flags and removes duplicates.
    """

    def _remove_opt(current: list[str], opt: str, *, takes_value: bool) -> list[str]:
        out: list[str] = []
        i = 0
        while i < len(current):
            if current[i] == opt:
                if takes_value and i + 1 < len(current):
                    i += 2
                else:
                    i += 1
                continue
            out.append(current[i])
            i += 1
        return out

    out = list(args)

    # Value options
    for key, opt in [
        ("bronze_dir", "--bronze-dir"),
        ("silver_dir", "--silver-dir"),
        ("max_availability_files", "--max-availability-files"),
        ("external_metro_stations_csv", "--external-metro-stations-csv"),
    ]:
        if overrides.get(key) is None:
            continue
        out = _remove_opt(out, opt, takes_value=True)
        out += [opt, str(overrides[key])]

    # Boolean flag
    if overrides.get("prefer_external_metro") is not None:
        out = _remove_opt(out, "--prefer-external-metro", takes_value=False)
        if bool(overrides["prefer_external_metro"]):
            out.append("--prefer-external-metro")

    # Allow raw args to be appended last (advanced usage).
    extra = overrides.get("args")
    if isinstance(extra, list) and extra:
        out += [str(x) for x in extra]

    return out


class JobManager:
    def __init__(self, *, repo_root: Path) -> None:
        self._repo_root = Path(repo_root)
        self._jobs_dir = self._repo_root / "logs" / "jobs"
        self._jobs_dir.mkdir(parents=True, exist_ok=True)
        self._jobs: dict[str, Job] = {}
        self._index_path = self._jobs_dir / "index.json"
        self._load_index()

    @property
    def jobs_dir(self) -> Path:
        return self._jobs_dir

    def list_jobs(self, *, limit: int = 20) -> list[Job]:
        jobs = list(self._jobs.values())
        jobs.sort(key=lambda j: j.created_at_utc, reverse=True)
        return jobs[: max(int(limit), 1)]

    def get(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    def _load_index(self) -> None:
        if not self._index_path.exists():
            return
        try:
            obj = json.loads(self._index_path.read_text(encoding="utf-8"))
        except Exception:
            return
        items = obj.get("jobs") if isinstance(obj, dict) else None
        if not isinstance(items, list):
            return
        for it in items:
            if not isinstance(it, dict):
                continue
            try:
                job_id = str(it.get("id") or "")
                if not job_id:
                    continue
                created = datetime.fromisoformat(str(it.get("created_at_utc"))).astimezone(timezone.utc)
                started = it.get("started_at_utc")
                started_dt = (
                    datetime.fromisoformat(str(started)).astimezone(timezone.utc) if started else None
                )
                finished = it.get("finished_at_utc")
                finished_dt = (
                    datetime.fromisoformat(str(finished)).astimezone(timezone.utc) if finished else None
                )
                pid = it.get("pid")
                pid_i = int(pid) if pid is not None else None
                rc = it.get("returncode")
                rc_i = int(rc) if rc is not None else None
                log_path = Path(str(it.get("log_path") or (self._jobs_dir / f"{job_id}.log")))
                cmd = it.get("command")
                cmd_list = (
                    list(cmd)
                    if isinstance(cmd, list)
                    else [sys.executable, str(self._repo_root / "scripts" / "build_silver_locked.py")]
                )
                kind = str(it.get("kind") or "build_silver")
            except Exception:
                continue
            self._jobs[job_id] = Job(
                id=job_id,
                kind=kind,
                created_at_utc=created,
                started_at_utc=started_dt,
                finished_at_utc=finished_dt,
                pid=pid_i,
                returncode=rc_i,
                log_path=log_path,
                cmd=cmd_list,
                persisted=True,
                _proc=None,
            )

    def _save_index(self) -> None:
        jobs = []
        for j in self.list_jobs(limit=10_000):
            jobs.append(
                {
                    "id": j.id,
                    "kind": j.kind,
                    "created_at_utc": j.created_at_utc.isoformat(),
                    "started_at_utc": None if j.started_at_utc is None else j.started_at_utc.isoformat(),
                    "finished_at_utc": None if j.finished_at_utc is None else j.finished_at_utc.isoformat(),
                    "pid": j.pid,
                    "returncode": j.returncode,
                    "log_path": str(j.log_path),
                    "command": list(j.cmd) if j.cmd else None,
                }
            )
        payload = {"updated_at_utc": datetime.now(timezone.utc).isoformat(), "jobs": jobs}
        tmp = self._index_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self._index_path)

    def start_build_silver(self, *, args: list[str] | None = None) -> Job:
        job_id = uuid.uuid4().hex
        now = datetime.now(timezone.utc)
        log_path = self._jobs_dir / f"{job_id}.log"

        scripts_dir = self._repo_root / "scripts"
        cmd = [sys.executable, str(scripts_dir / "build_silver_locked.py")]
        if args:
            cmd += list(args)

        job = Job(
            id=job_id,
            kind="build_silver",
            created_at_utc=now,
            started_at_utc=datetime.now(timezone.utc),
            finished_at_utc=None,
            pid=None,
            returncode=None,
            log_path=log_path,
            cmd=cmd,
            _proc=None,
        )

        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("ab") as f:
            proc = subprocess.Popen(
                cmd,
                cwd=str(self._repo_root),
                stdout=f,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                env=os.environ.copy(),
            )
        job._proc = proc
        job.pid = proc.pid
        self._jobs[job_id] = job
        self._save_index()
        return job

    def cancel(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if job is None:
            return False
        if job.pid is None:
            return False
        try:
            os.killpg(job.pid, signal.SIGTERM)
        except Exception:
            try:
                os.kill(job.pid, signal.SIGTERM)
            except Exception:
                return False
        job.finished_at_utc = datetime.now(timezone.utc)
        job.returncode = -signal.SIGTERM
        job.persisted = True
        self._save_index()
        return True

    def to_public_dict(self, job: Job) -> dict[str, object]:
        status = job.status
        # Persist returncode/finish time if we just observed completion.
        if job._proc is not None and status in {"succeeded", "failed"} and not job.persisted:
            job.persisted = True
            self._save_index()
        tail = _tail_lines(job.log_path, max_lines=30)
        stage: str | None = None
        progress: int | None = None
        if job.kind == "build_silver":
            ev = _parse_mba_events(tail)
            if ev is not None:
                stage = str(ev.get("stage") or "") or None
                try:
                    progress = int(ev.get("progress_pct")) if ev.get("progress_pct") is not None else None
                except Exception:
                    progress = None
            if stage is None or progress is None:
                stage, progress = _infer_build_silver_progress(tail)
        return {
            "id": job.id,
            "kind": job.kind,
            "status": status,
            "stage": stage,
            "progress_pct": progress,
            "created_at_utc": job.created_at_utc,
            "started_at_utc": job.started_at_utc,
            "finished_at_utc": job.finished_at_utc,
            "pid": job.pid,
            "returncode": job.returncode,
            "log_path": str(job.log_path),
            "stdout_tail": tail,
            "command": list(job.cmd) if job.cmd else None,
        }

    def rerun(self, job_id: str, *, overrides: dict[str, object] | None = None) -> Job | None:
        job = self._jobs.get(job_id)
        if job is None:
            return None
        if not job.cmd:
            return None
        # Re-run the same script arguments (skip python + script path).
        args = job.cmd[2:] if len(job.cmd) >= 2 else []
        if overrides:
            args = apply_build_silver_overrides(args, overrides)
        return self.start_build_silver(args=args)
