from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class LinearRegressionResult:
    intercept: float
    coefficients: dict[str, float]
    r2: float
    n: int


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


def fit_linear_regression(
    station_features: pd.DataFrame,
    station_targets: pd.DataFrame,
    *,
    target_metric: str,
    feature_cols: Optional[list[str]] = None,
) -> LinearRegressionResult:
    """
    Fit a simple OLS regression: y ~ 1 + X (for quick EDA only).
    """

    features = station_features.copy()
    features["station_id"] = features["station_id"].astype(str)

    targets = station_targets.copy()
    targets["station_id"] = targets["station_id"].astype(str)
    targets = targets[targets["metric"] == target_metric].copy()

    joined = features.merge(targets[["station_id", "value"]], on="station_id", how="inner")
    cols = feature_cols or _numeric_feature_cols(joined)
    if not cols:
        return LinearRegressionResult(intercept=float("nan"), coefficients={}, r2=float("nan"), n=0)

    data = joined[cols + ["value"]].dropna()
    if len(data) < 3:
        return LinearRegressionResult(intercept=float("nan"), coefficients={}, r2=float("nan"), n=int(len(data)))

    x = data[cols].to_numpy(dtype=float)
    y = data["value"].to_numpy(dtype=float)

    x_mean = x.mean(axis=0)
    x_std = x.std(axis=0)
    x_std = np.where(x_std == 0, 1.0, x_std)
    xz = (x - x_mean) / x_std

    x_design = np.column_stack([np.ones(len(xz)), xz])
    beta, *_ = np.linalg.lstsq(x_design, y, rcond=None)
    y_hat = x_design @ beta

    ss_res = float(np.sum((y - y_hat) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else float("nan")

    intercept = float(beta[0])
    coefs = {col: float(beta[i + 1]) for i, col in enumerate(cols)}
    return LinearRegressionResult(intercept=intercept, coefficients=coefs, r2=r2, n=int(len(y)))
