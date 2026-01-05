from __future__ import annotations

import math

import pandas as pd

from metrobikeatlas.analytics.correlation import compute_feature_correlations
from metrobikeatlas.analytics.kmeans import kmeans_cluster
from metrobikeatlas.analytics.linear_regression import fit_linear_regression
from metrobikeatlas.analytics.similarity import find_similar_stations


def test_similarity_euclidean_orders_by_distance() -> None:
    features = pd.DataFrame(
        [
            {"station_id": "A", "x": 0.0, "y": 0.0},
            {"station_id": "B", "x": 1.0, "y": 0.0},
            {"station_id": "C", "x": 10.0, "y": 0.0},
        ]
    )
    out = find_similar_stations(features, station_id="A", top_k=2, metric="euclidean", standardize=False)
    assert out["station_id"].tolist() == ["B", "C"]
    assert out["distance"].iloc[0] < out["distance"].iloc[1]


def test_correlation_detects_perfect_linear_relationship() -> None:
    features = pd.DataFrame(
        [
            {"station_id": "A", "f1": 1.0, "f2": 0.0},
            {"station_id": "B", "f1": 2.0, "f2": 0.0},
            {"station_id": "C", "f1": 3.0, "f2": 0.0},
            {"station_id": "D", "f1": 4.0, "f2": 0.0},
        ]
    )
    targets = pd.DataFrame(
        [
            {"station_id": "A", "metric": "t", "value": 2.0},
            {"station_id": "B", "metric": "t", "value": 4.0},
            {"station_id": "C", "metric": "t", "value": 6.0},
            {"station_id": "D", "metric": "t", "value": 8.0},
        ]
    )
    corr = compute_feature_correlations(features, targets, target_metric="t")
    f1 = corr[corr["feature"] == "f1"].iloc[0]
    assert math.isclose(float(f1["correlation"]), 1.0, rel_tol=1e-9, abs_tol=1e-9)


def test_linear_regression_r2_is_one_for_perfect_fit() -> None:
    features = pd.DataFrame(
        [
            {"station_id": "A", "f1": 1.0},
            {"station_id": "B", "f1": 2.0},
            {"station_id": "C", "f1": 3.0},
            {"station_id": "D", "f1": 4.0},
        ]
    )
    targets = pd.DataFrame(
        [
            {"station_id": "A", "metric": "t", "value": 2.0},
            {"station_id": "B", "metric": "t", "value": 4.0},
            {"station_id": "C", "metric": "t", "value": 6.0},
            {"station_id": "D", "metric": "t", "value": 8.0},
        ]
    )
    res = fit_linear_regression(features, targets, target_metric="t")
    assert math.isclose(res.r2, 1.0, rel_tol=1e-9, abs_tol=1e-9)
    assert res.coefficients["f1"] > 0


def test_kmeans_cluster_returns_labels() -> None:
    features = pd.DataFrame(
        [
            {"station_id": "A", "x": 0.0},
            {"station_id": "B", "x": 0.1},
            {"station_id": "C", "x": 10.0},
            {"station_id": "D", "x": 10.2},
        ]
    )
    result = kmeans_cluster(features, k=2, standardize=False, random_state=0)
    assert set(result.labels.columns) == {"station_id", "cluster"}
    assert len(result.labels) == 4
    assert result.labels["cluster"].nunique() <= 2

