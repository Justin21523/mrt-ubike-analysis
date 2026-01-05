from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd


def _numeric_feature_cols(
    df: pd.DataFrame, *, exclude: Iterable[str] = ("station_id", "district", "value")
) -> list[str]:
    excluded = set(exclude)
    cols = []
    for col in df.columns:
        if col in excluded:
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            cols.append(col)
    return cols


def compute_feature_correlations(
    station_features: pd.DataFrame,
    station_targets: pd.DataFrame,
    *,
    target_metric: str,
    feature_cols: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Compute Pearson correlation between each numeric feature and the target across stations.
    """

    features = station_features.copy()
    features["station_id"] = features["station_id"].astype(str)

    targets = station_targets.copy()
    targets["station_id"] = targets["station_id"].astype(str)
    targets = targets[targets["metric"] == target_metric].copy()

    joined = features.merge(targets[["station_id", "value"]], on="station_id", how="inner")
    if joined.empty:
        return pd.DataFrame(columns=["feature", "correlation", "n"])

    cols = feature_cols or _numeric_feature_cols(joined)
    results = []
    for col in cols:
        pair = joined[[col, "value"]].dropna()
        if len(pair) < 3:
            continue
        corr = float(pair[col].corr(pair["value"]))
        results.append({"feature": col, "correlation": corr, "n": int(len(pair))})

    out = pd.DataFrame(results)
    if out.empty:
        return out
    out = out.sort_values("correlation", ascending=False).reset_index(drop=True)
    return out
