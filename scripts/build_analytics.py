from __future__ import annotations

import sys
from pathlib import Path

# Allow running scripts without requiring an editable install (`pip install -e .`).
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

import argparse
import logging
import json
from datetime import datetime, timezone

import pandas as pd

from metrobikeatlas.analytics.correlation import compute_feature_correlations
from metrobikeatlas.analytics.kmeans import kmeans_cluster
from metrobikeatlas.analytics.linear_regression import fit_linear_regression
from metrobikeatlas.config.loader import load_config
from metrobikeatlas.utils.logging import configure_logging


logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-metric", default="metro_flow_proxy_from_bike_rent")
    parser.add_argument("--out-dir", default="data/gold")
    args = parser.parse_args()

    config = load_config()
    configure_logging(config.logging)

    if not config.features.station_features_path.exists():
        raise FileNotFoundError(f"Missing station features: {config.features.station_features_path}")
    if not config.features.station_targets_path.exists():
        raise FileNotFoundError(f"Missing station targets: {config.features.station_targets_path}")

    features = pd.read_csv(config.features.station_features_path)
    targets = pd.read_csv(config.features.station_targets_path)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    corr = compute_feature_correlations(features, targets, target_metric=args.target_metric)
    corr_path = out_dir / "feature_correlations.csv"
    corr.to_csv(corr_path, index=False)
    logger.info("Wrote %s", corr_path)

    reg = fit_linear_regression(features, targets, target_metric=args.target_metric)
    reg_df = pd.DataFrame(
        [{"feature": k, "coefficient": v} for k, v in reg.coefficients.items()]
        + [{"feature": "__intercept__", "coefficient": reg.intercept}]
    )
    reg_df["r2"] = reg.r2
    reg_df["n"] = reg.n
    reg_path = out_dir / "regression_coefficients.csv"
    reg_df.to_csv(reg_path, index=False)
    logger.info("Wrote %s", reg_path)

    kmeans = kmeans_cluster(
        features,
        k=config.analytics.clustering.k,
        standardize=config.analytics.clustering.standardize,
    )
    cluster_path = out_dir / "station_clusters.csv"
    kmeans.labels.to_csv(cluster_path, index=False)
    logger.info("Wrote %s", cluster_path)

    # Reproducibility meta (ties analytics outputs back to Silver build id/hash + features meta).
    repo_root = Path(__file__).resolve().parents[1]
    silver_meta_path = repo_root / "data" / "silver" / "_build_meta.json"
    try:
        silver_meta = json.loads(silver_meta_path.read_text(encoding="utf-8")) if silver_meta_path.exists() else None
    except Exception:
        silver_meta = None
    run_meta = {
        "type": "gold_run_meta",
        "stage": "analytics",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "silver_build_id": (silver_meta or {}).get("build_id") if isinstance(silver_meta, dict) else None,
        "silver_inputs_hash": (silver_meta or {}).get("inputs_hash") if isinstance(silver_meta, dict) else None,
        "inputs": {"target_metric": args.target_metric},
        "artifacts": {
            "feature_correlations": str(corr_path),
            "regression_coefficients": str(reg_path),
            "station_clusters": str(cluster_path),
        },
    }
    (out_dir / "_run_meta.json").write_text(json.dumps(run_meta, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Wrote %s", out_dir / "_run_meta.json")


if __name__ == "__main__":
    main()
