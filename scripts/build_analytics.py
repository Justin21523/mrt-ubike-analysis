from __future__ import annotations

import argparse
import logging
from pathlib import Path

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


if __name__ == "__main__":
    main()

