from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from metrobikeatlas.config.loader import load_config
from metrobikeatlas.ingestion.bronze import write_bronze_json
from metrobikeatlas.ingestion.tdx_base import TDXClient, TDXCredentials
from metrobikeatlas.utils.logging import configure_logging


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect a single bike availability snapshot from TDX (Bronze).")
    parser.add_argument("--config", default=None, help="Config JSON path.")
    parser.add_argument("--bronze-dir", default="data/bronze")
    args = parser.parse_args()

    config = load_config(args.config)
    configure_logging(config.logging)
    creds = TDXCredentials.from_env()

    bronze_dir = Path(args.bronze_dir)
    bronze_dir.mkdir(parents=True, exist_ok=True)

    with TDXClient(
        base_url=config.tdx.base_url,
        token_url=config.tdx.token_url,
        credentials=creds,
    ) as tdx:
        for city in config.tdx.bike.cities:
            path = config.tdx.bike.availability_path_template.format(city=city)
            params = {"$format": "JSON"}
            retrieved_at = datetime.now(timezone.utc)
            payload = tdx.get_json(path, params=params)

            out = write_bronze_json(
                bronze_dir,
                source="tdx",
                domain="bike",
                dataset="availability",
                city=city,
                retrieved_at=retrieved_at,
                request={"path": path, "params": params},
                payload=payload,
            )
            print(f"Wrote {out}")


if __name__ == "__main__":
    main()
