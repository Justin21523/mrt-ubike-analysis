from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from metrobikeatlas.config.loader import load_config
from metrobikeatlas.ingestion.bronze import write_bronze_json
from metrobikeatlas.ingestion.tdx_base import TDXClient, TDXCredentials


def main() -> None:
    config = load_config()
    creds = TDXCredentials.from_env()

    bronze_dir = Path("data/bronze")
    bronze_dir.mkdir(parents=True, exist_ok=True)

    with TDXClient(
        base_url=config.tdx.base_url,
        token_url=config.tdx.token_url,
        credentials=creds,
    ) as tdx:
        for city in config.tdx.bike.cities:
            path = config.tdx.bike.stations_path_template.format(city=city)
            params = {"$format": "JSON"}
            retrieved_at = datetime.now(timezone.utc)
            payload = tdx.get_json(path, params=params)

            out = write_bronze_json(
                bronze_dir,
                source="tdx",
                domain="bike",
                dataset="stations",
                city=city,
                retrieved_at=retrieved_at,
                request={"path": path, "params": params},
                payload=payload,
            )
            print(f"Wrote {out}")


if __name__ == "__main__":
    main()

