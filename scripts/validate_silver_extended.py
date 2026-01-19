from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
import urllib.request
from pathlib import Path

from metrobikeatlas.quality.contract import compute_schema_meta, validate_silver_extended, write_json


def _notify(url: str, payload: dict[str, object]) -> None:
    data = __import__("json").dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, method="POST", data=data)
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=3.0) as _resp:
        return


def main() -> None:
    p = argparse.ArgumentParser(description="Extended Silver DQ checks + schema meta output.")
    p.add_argument("--silver-dir", default="data/silver")
    p.add_argument("--out", default="logs/dq/latest.json")
    p.add_argument("--strict", action="store_true")
    args = p.parse_args()

    silver_dir = Path(args.silver_dir)
    issues = validate_silver_extended(silver_dir, strict=False)
    schema = compute_schema_meta(silver_dir)

    report = {
        "type": "dq_report",
        "scope": "silver_extended",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "silver_dir": str(silver_dir),
        "issues": [{"level": i.level, "table": i.table, "message": i.message} for i in issues],
        "ok": not any(i.level == "error" for i in issues),
        "schema_meta": schema,
    }
    out = Path(args.out)
    write_json(out, report)
    print(f"Wrote {out}")

    # Critical-only notify (reuse the same webhook env as the API).
    url = os.getenv("METROBIKEATLAS_ALERT_WEBHOOK_URL")
    if url and any(i.level == "error" for i in issues):
        try:
            critical = [i for i in issues if i.level == "error"][:5]
            text = "Silver DQ critical:\n" + "\n".join(f"- {i.table}: {i.message}" for i in critical)
            kind = (os.getenv("METROBIKEATLAS_ALERT_WEBHOOK_KIND") or "").strip().lower()
            if not kind:
                if "slack.com" in url:
                    kind = "slack"
                elif "discord.com" in url or "discordapp.com" in url:
                    kind = "discord"
                else:
                    kind = "generic"
            payload = {"text": text} if kind == "slack" else {"content": text} if kind == "discord" else report
            _notify(url, payload)
        except Exception:
            pass

    if args.strict and not report["ok"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()

