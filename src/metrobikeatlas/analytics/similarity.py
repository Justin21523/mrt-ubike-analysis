from __future__ import annotations

import math
from typing import Iterable, Literal, Optional

import numpy as np
import pandas as pd


SimilarityMetric = Literal["euclidean", "cosine"]


def _numeric_feature_cols(df: pd.DataFrame, *, exclude: Iterable[str] = ("station_id", "district")) -> list[str]:
    excluded = set(exclude)
    cols = []
    for col in df.columns:
        if col in excluded:
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            cols.append(col)
    return cols


def _prepare_matrix(
    features: pd.DataFrame,
    *,
    feature_cols: Optional[list[str]] = None,
    standardize: bool = True,
) -> tuple[np.ndarray, list[str]]:
    cols = feature_cols or _numeric_feature_cols(features)
    if not cols:
        raise ValueError("No numeric feature columns found")

    x = features[cols].to_numpy(dtype=float)
    col_means = np.nanmean(x, axis=0)
    x = np.where(np.isnan(x), col_means, x)

    if standardize:
        mu = x.mean(axis=0)
        sigma = x.std(axis=0)
        sigma = np.where(sigma == 0, 1.0, sigma)
        x = (x - mu) / sigma

    return x, cols


def find_similar_stations(
    station_features: pd.DataFrame,
    *,
    station_id: str,
    top_k: int = 5,
    metric: SimilarityMetric = "euclidean",
    standardize: bool = True,
    feature_cols: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Find the most similar stations in feature space.

    Returns a DataFrame with columns: `station_id`, `distance`.
    """

    if "station_id" not in station_features.columns:
        raise ValueError("station_features missing required column: station_id")

    features = station_features.copy()
    features["station_id"] = features["station_id"].astype(str)
    if station_id not in set(features["station_id"]):
        raise KeyError(station_id)

    x, cols = _prepare_matrix(features, feature_cols=feature_cols, standardize=standardize)
    ids = features["station_id"].to_numpy(dtype=str)
    idx = int(np.where(ids == station_id)[0][0])

    v = x[idx]
    if metric == "euclidean":
        d = np.linalg.norm(x - v, axis=1)
    elif metric == "cosine":
        denom = (np.linalg.norm(x, axis=1) * max(np.linalg.norm(v), 1e-12))
        sim = (x @ v) / np.where(denom == 0, 1.0, denom)
        d = 1.0 - sim
    else:
        raise ValueError(f"Unsupported metric: {metric}")

    out = pd.DataFrame({"station_id": ids, "distance": d})
    out = out[out["station_id"] != station_id]
    out = out.sort_values("distance").head(max(int(top_k), 0))
    out["distance"] = out["distance"].astype(float)
    return out.reset_index(drop=True)

