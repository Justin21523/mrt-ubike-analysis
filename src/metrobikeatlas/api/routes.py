from __future__ import annotations

# We use `Literal` to restrict certain query parameters to a small, documented set of values.
# We use `Optional[...]` for parameters that can be omitted so the server can fall back to config defaults.
from typing import Literal, Optional

# FastAPI primitives:
# - `APIRouter` groups endpoints (routes) so the app factory can include them cleanly.
# - `Depends` performs dependency injection (DI) per request (no global variables needed).
# - `HTTPException` converts Python errors into proper HTTP status codes + JSON error payloads.
# - `Request` gives access to `app.state` where we store our service object.
from fastapi import APIRouter, Depends, HTTPException, Request

# Pydantic response models:
# These models define the JSON schema returned to the browser and validate payload shape at runtime.
# This matters for maintainability: the frontend (DOM + fetch) can rely on stable fields and types.
from metrobikeatlas.api.schemas import (
    AnalyticsOverviewOut,
    AppConfigOut,
    BikeStationOut,
    NearbyBikeOut,
    SimilarStationOut,
    StationFactorsOut,
    StationOut,
    StationTimeSeriesOut,
)

# `StationService` is our thin application layer that hides whether we are in demo mode or real-data mode.
# Dataflow: HTTP request -> route handler -> StationService -> repository -> dict payload -> Pydantic model -> JSON response.
from metrobikeatlas.api.service import StationService


# A router is like a "mini app": it holds endpoints that can be attached to a FastAPI application.
router = APIRouter()


# Dependency provider: FastAPI will call this function per request when a handler declares `Depends(get_service)`.
# We keep the service on `app.state` so it is constructed once in the app factory (not per request).
def get_service(request: Request) -> StationService:
    # `app.state` is a generic container, so mypy cannot know the attribute exists; hence the `type: ignore`.
    # Pitfall: if `StationService` is not attached in `create_app`, this will raise `AttributeError` at runtime.
    return request.app.state.station_service  # type: ignore[attr-defined]


# Config endpoint: the dashboard uses this to initialize UI defaults (e.g., join radius, granularity).
@router.get("/config", response_model=AppConfigOut)
def get_config(service: StationService = Depends(get_service)) -> AppConfigOut:
    # Read a snapshot of the typed config so the frontend can render defaults consistently with the backend.
    cfg = service.config
    # Return a Pydantic model instance; FastAPI will serialize it to JSON for the browser.
    return AppConfigOut(
        # Basic app metadata helps the UI show the correct title and which mode it is running in.
        app_name=cfg.app.name,
        demo_mode=cfg.app.demo_mode,
        # Temporal config controls how time series are aligned for visualization (15min/hour/day, timezone).
        temporal={
            "timezone": cfg.temporal.timezone,
            "granularity": cfg.temporal.granularity,
        },
        # Spatial config controls how bike stations are associated with metro stations (buffer radius vs nearest K).
        spatial={
            "join_method": cfg.spatial.join_method,
            "radius_m": cfg.spatial.radius_m,
            "nearest_k": cfg.spatial.nearest_k,
        },
        # Analytics config controls similarity and clustering (used by "similar stations" in the UI).
        analytics={
            "similarity": {
                "top_k": cfg.analytics.similarity.top_k,
                "metric": cfg.analytics.similarity.metric,
                "standardize": cfg.analytics.similarity.standardize,
            },
            "clustering": {
                "k": cfg.analytics.clustering.k,
                "standardize": cfg.analytics.clustering.standardize,
            },
        },
        # Map defaults let the UI start at a sensible location without hard-coding values in JS.
        web_map={
            "center_lat": cfg.web.map.center_lat,
            "center_lon": cfg.web.map.center_lon,
            "zoom": cfg.web.map.zoom,
        },
    )


# Bike station metadata endpoint (used for overlays and debug tooling in the dashboard).
@router.get("/bike_stations", response_model=list[BikeStationOut])
def list_bike_stations(service: StationService = Depends(get_service)) -> list[BikeStationOut]:
    # Fetch the raw list from the repository via the service layer.
    bikes = service.list_bike_stations()
    # Convert internal dicts to a stable output schema (`BikeStationOut`) for the frontend.
    return [
        BikeStationOut(
            id=s["station_id"],
            name=s["name"],
            lat=s["lat"],
            lon=s["lon"],
            city=s.get("city"),
            operator=s.get("operator"),
            capacity=s.get("capacity"),
        )
        for s in bikes
    ]


# Metro station metadata endpoint (used to render markers on the Leaflet map in the browser).
@router.get("/stations", response_model=list[StationOut])
def list_stations(service: StationService = Depends(get_service)) -> list[StationOut]:
    # Fetch metro stations (optionally enriched with district/cluster if Gold tables are present).
    stations = service.list_stations()
    # Map internal dict keys to the public API field names expected by the frontend.
    return [
        StationOut(
            id=s["station_id"],
            name=s["name"],
            lat=s["lat"],
            lon=s["lon"],
            city=s.get("city"),
            system=s.get("system"),
            district=s.get("district"),
            cluster=s.get("cluster"),
        )
        for s in stations
    ]


# Timeseries endpoint: returns aligned metro + bike series for a selected metro station.
# The dashboard passes query params so the user can adjust parameters (granularity, radius, etc.) at runtime.
@router.get("/station/{station_id}/timeseries", response_model=StationTimeSeriesOut)
def station_timeseries(
    # Path parameter: the unique metro station id (used as a stable key across tables).
    station_id: str,
    # Query params: spatial join controls for which bike stations get aggregated.
    join_method: Optional[Literal["buffer", "nearest"]] = None,
    radius_m: Optional[float] = None,
    nearest_k: Optional[int] = None,
    # Query params: temporal alignment controls for bucketing timestamps.
    granularity: Optional[Literal["15min", "hour", "day"]] = None,
    timezone: Optional[str] = None,
    window_days: Optional[int] = None,
    # Query param: choose whether to prefer real ridership or a bike-derived proxy in the response.
    metro_series: Literal["auto", "ridership", "proxy"] = "auto",
    # Dependency injection: FastAPI calls `get_service` and passes the result here.
    service: StationService = Depends(get_service),
) -> StationTimeSeriesOut:
    try:
        # Delegate to the service layer so route code stays "thin" and focused on HTTP concerns.
        payload = service.station_timeseries(
            station_id,
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            granularity=granularity,
            timezone=timezone,
            window_days=window_days,
            metro_series=metro_series,
        )
    except KeyError:
        # A missing station id maps to 404 so the frontend can show a "not found" message.
        raise HTTPException(status_code=404, detail="Station not found")
    except ValueError as e:
        # Bad user input (e.g., unsupported granularity) maps to 400 for a clear client-side error.
        # Note: FastAPI may also return 422 for validation errors before this handler runs.
        raise HTTPException(status_code=400, detail=str(e))
    # Validate the payload shape against the Pydantic model (defensive programming for API stability).
    return StationTimeSeriesOut.model_validate(payload)


# Nearby bike endpoint: returns bike stations associated with a metro station under the chosen join parameters.
@router.get("/station/{station_id}/nearby_bike", response_model=list[NearbyBikeOut])
def nearby_bike(
    # Path parameter: selected metro station.
    station_id: str,
    # Query params: choose join method and its parameterization.
    join_method: Optional[Literal["buffer", "nearest"]] = None,
    radius_m: Optional[float] = None,
    nearest_k: Optional[int] = None,
    # Query param: limit keeps payload small (important for UI performance).
    limit: Optional[int] = None,
    # Dependency injection: provides access to our service/repository without globals.
    service: StationService = Depends(get_service),
) -> list[NearbyBikeOut]:
    try:
        # Delegate the selection logic (buffer / nearest) to the repository via the service layer.
        payload = service.nearby_bike(
            station_id,
            join_method=join_method,
            radius_m=radius_m,
            nearest_k=nearest_k,
            limit=limit,
        )
    except KeyError:
        # Unknown station id -> 404.
        raise HTTPException(status_code=404, detail="Station not found")
    except ValueError as e:
        # Invalid join settings -> 400.
        raise HTTPException(status_code=400, detail=str(e))
    # Convert each dict into a validated response object so the frontend gets predictable fields.
    return [
        NearbyBikeOut(
            id=s["station_id"],
            name=s["name"],
            lat=s["lat"],
            lon=s["lon"],
            distance_m=s["distance_m"],
            capacity=s.get("capacity"),
        )
        for s in payload
    ]


# Station factors endpoint: returns the feature table row for the station (if Gold features exist).
@router.get("/station/{station_id}/factors", response_model=StationFactorsOut)
def station_factors(
    station_id: str, service: StationService = Depends(get_service)
) -> StationFactorsOut:
    try:
        # Features are computed offline (Gold) and served here for UI inspection.
        payload = service.station_factors(station_id)
    except KeyError:
        # Unknown station id -> 404.
        raise HTTPException(status_code=404, detail="Station not found")
    # Validate payload and return; the UI uses this to render the factors table in the DOM.
    return StationFactorsOut.model_validate(payload)


# Similar stations endpoint: returns a k-nearest list in feature space (for quick exploration).
@router.get("/station/{station_id}/similar", response_model=list[SimilarStationOut])
def similar_stations(
    # Path parameter: anchor station id.
    station_id: str,
    # Query params: runtime overrides for similarity settings (useful for experimentation).
    top_k: Optional[int] = None,
    metric: Optional[Literal["euclidean", "cosine"]] = None,
    standardize: Optional[bool] = None,
    # Dependency injection: provides the service.
    service: StationService = Depends(get_service),
) -> list[SimilarStationOut]:
    try:
        # Delegate the similarity computation (or lookup) to the repository layer.
        payload = service.similar_stations(
            station_id,
            top_k=top_k,
            metric=metric,
            standardize=standardize,
        )
    except KeyError:
        # Unknown station id -> 404.
        raise HTTPException(status_code=404, detail="Station not found")
    except ValueError as e:
        # Invalid similarity params -> 400.
        raise HTTPException(status_code=400, detail=str(e))
    # Map dict payload into the public schema consumed by the UI.
    return [
        SimilarStationOut(
            id=s["station_id"],
            name=s.get("name"),
            distance=s["distance"],
            cluster=s.get("cluster"),
        )
        for s in payload
    ]


# Analytics overview endpoint: returns precomputed global stats (correlations/regression/clusters) if present.
@router.get("/analytics/overview", response_model=AnalyticsOverviewOut)
def analytics_overview(service: StationService = Depends(get_service)) -> AnalyticsOverviewOut:
    # This is a lightweight endpoint so the UI can show a small summary without loading all Gold tables.
    payload = service.analytics_overview()
    # Validate and return; the UI renders this into the sidebar list.
    return AnalyticsOverviewOut.model_validate(payload)
