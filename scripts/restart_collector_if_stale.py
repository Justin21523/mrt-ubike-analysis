from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


def _post_json(url: str, payload: dict[str, object], *, admin_token: str | None = None, timeout_s: float = 5.0) -> dict[str, object]:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, method="POST", data=body)
    req.add_header("Content-Type", "application/json")
    if admin_token:
        req.add_header("X-Admin-Token", admin_token)
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            obj = json.loads(raw) if raw else {}
        except Exception:
            obj = {"detail": raw or str(e)}
        raise RuntimeError(f"HTTP {e.code}: {obj.get('detail') or raw}") from e


def main() -> int:
    parser = argparse.ArgumentParser(description="Restart MetroBikeAtlas collector only if stale.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--stale-after-seconds", type=int, default=900)
    parser.add_argument("--force", action="store_true", help="Restart even if heartbeat looks healthy.")
    parser.add_argument("--availability-interval-seconds", type=int, default=600)
    parser.add_argument("--stations-refresh-interval-hours", type=float, default=24.0)
    parser.add_argument("--jitter-seconds", type=float, default=5.0)
    parser.add_argument("--build-silver-interval-seconds", type=int, default=1800)
    parser.add_argument(
        "--admin-token",
        default=None,
        help="Optional METROBIKEATLAS_ADMIN_TOKEN (for non-localhost usage).",
    )
    args = parser.parse_args()

    admin_token = args.admin_token or os.getenv("METROBIKEATLAS_ADMIN_TOKEN")
    url = args.base_url.rstrip("/") + "/admin/collector/restart_if_stale"
    url += f"?stale_after_seconds={int(args.stale_after_seconds)}"
    if args.force:
        url += "&force=true"

    payload: dict[str, object] = {
        "availability_interval_seconds": int(args.availability_interval_seconds),
        "stations_refresh_interval_hours": float(args.stations_refresh_interval_hours),
        "jitter_seconds": float(args.jitter_seconds),
        "build_silver_interval_seconds": int(args.build_silver_interval_seconds),
    }

    res = _post_json(url, payload, admin_token=admin_token, timeout_s=5.0)
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0 if res.get("ok", True) else 2


if __name__ == "__main__":
    raise SystemExit(main())

