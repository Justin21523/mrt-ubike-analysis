from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class KMeansResult:
    labels: pd.DataFrame  # station_id, cluster
    centroids: np.ndarray


def _numeric_feature_cols(df: pd.DataFrame, *, exclude: Iterable[str] = ("station_id", "district")) -> list[str]:
    excluded = set(exclude)
    cols = []
    for col in df.columns:
        if col in excluded:
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            cols.append(col)
    return cols


def _standardize(x: np.ndarray) -> np.ndarray:
    mu = x.mean(axis=0)
    sigma = x.std(axis=0)
    sigma = np.where(sigma == 0, 1.0, sigma)
    return (x - mu) / sigma


def kmeans_cluster(
    station_features: pd.DataFrame,
    *,
    k: int,
    standardize: bool = True,
    feature_cols: Optional[list[str]] = None,
    random_state: int = 0,
    n_init: int = 5,
    max_iter: int = 100,
) -> KMeansResult:
    """
    Simple K-means for MVP clustering (no sklearn dependency).
    """

    if "station_id" not in station_features.columns:
        raise ValueError("station_features missing required column: station_id")

    df = station_features.copy()
    df["station_id"] = df["station_id"].astype(str)

    cols = feature_cols or _numeric_feature_cols(df)
    if not cols:
        return KMeansResult(labels=pd.DataFrame(columns=["station_id", "cluster"]), centroids=np.empty((0, 0)))

    data = df[["station_id"] + cols].dropna()
    if data.empty:
        return KMeansResult(labels=pd.DataFrame(columns=["station_id", "cluster"]), centroids=np.empty((0, 0)))

    ids = data["station_id"].to_numpy(dtype=str)
    x = data[cols].to_numpy(dtype=float)
    col_means = np.nanmean(x, axis=0)
    x = np.where(np.isnan(x), col_means, x)
    if standardize:
        x = _standardize(x)

    k = max(1, min(int(k), len(x)))
    rng = np.random.default_rng(random_state)

    best_inertia = float("inf")
    best_labels = None
    best_centroids = None

    for _ in range(max(int(n_init), 1)):
        init_idx = rng.choice(len(x), size=k, replace=False)
        centroids = x[init_idx].copy()

        for _ in range(max(int(max_iter), 1)):
            distances = np.linalg.norm(x[:, None, :] - centroids[None, :, :], axis=2)
            labels = distances.argmin(axis=1)

            new_centroids = np.vstack(
                [
                    x[labels == j].mean(axis=0) if np.any(labels == j) else centroids[j]
                    for j in range(k)
                ]
            )
            if np.allclose(new_centroids, centroids):
                centroids = new_centroids
                break
            centroids = new_centroids

        inertia = float(np.sum((x - centroids[labels]) ** 2))
        if inertia < best_inertia:
            best_inertia = inertia
            best_labels = labels.copy()
            best_centroids = centroids.copy()

    labels_df = pd.DataFrame({"station_id": ids, "cluster": best_labels.astype(int)})
    return KMeansResult(labels=labels_df, centroids=best_centroids)

