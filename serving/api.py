from __future__ import annotations

import os
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import FileResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from observability.metrics import api_metrics
from serving.repository import ServingConflictError, ServingNotFoundError, ServingRepository


DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://citibike:citibike_local_only@postgres:5432/citibike"
)
DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "dashboard")
LOGGER = logging.getLogger("citibike-serving-api")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(message)s")
DATABASE_POOL = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=int(os.environ.get("DATABASE_POOL_MIN_SIZE", "2")),
    max_size=int(os.environ.get("DATABASE_POOL_MAX_SIZE", "16")),
    timeout=float(os.environ.get("DATABASE_POOL_TIMEOUT_SECONDS", "5")),
    kwargs={"row_factory": dict_row},
    open=False,
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    DATABASE_POOL.open(wait=True)
    try:
        yield
    finally:
        DATABASE_POOL.close()

app = FastAPI(
    title="Citi Bike Station Operations API",
    version="1.0.0",
    description="Read-only station/alert serving endpoints plus an idempotent acknowledge command.",
    lifespan=lifespan,
)
app.mount("/dashboard/static", StaticFiles(directory=DASHBOARD_DIR), name="dashboard-static")


@app.middleware("http")
async def request_log(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception as exc:
        duration_seconds = time.perf_counter() - started
        route = getattr(request.scope.get("route"), "path", "__unmatched__")
        if request.url.path != "/metrics":
            api_metrics.observe(request.method, route, 500, duration_seconds)
        LOGGER.error(
            json.dumps(
                {
                    "timestamp_utc": utc_now(),
                    "level": "ERROR",
                    "service": "serving-api",
                    "event": "request_failed",
                    "message": "Unhandled API request failure",
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "error_type": type(exc).__name__,
                },
                separators=(",", ":"),
            )
        )
        raise
    duration_seconds = time.perf_counter() - started
    route = getattr(request.scope.get("route"), "path", "__unmatched__")
    if request.url.path != "/metrics":
        api_metrics.observe(request.method, route, response.status_code, duration_seconds)
    response.headers["x-request-id"] = request_id
    response.headers["x-content-type-options"] = "nosniff"
    response.headers["x-frame-options"] = "DENY"
    response.headers["referrer-policy"] = "no-referrer"
    response.headers["content-security-policy"] = (
        "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; "
        "script-src 'self'; connect-src 'self'; frame-ancestors 'none'"
    )
    LOGGER.info(
        json.dumps(
            {
                "timestamp_utc": utc_now(),
                "level": "INFO",
                "service": "serving-api",
                "event": "request_complete",
                "message": "API request completed",
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": round(duration_seconds * 1000, 2),
            },
            separators=(",", ":"),
        )
    )
    return response


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/dashboard")


@app.get("/dashboard", include_in_schema=False)
def dashboard() -> FileResponse:
    return FileResponse(os.path.join(DASHBOARD_DIR, "index.html"))


class AcknowledgeRequest(BaseModel):
    idempotency_key: str = Field(min_length=8, max_length=128)
    requested_by: str = Field(min_length=1, max_length=128)
    acknowledged_at_utc: datetime | None = None

    @field_validator("acknowledged_at_utc")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("acknowledged_at_utc must include a timezone")
        return value


def repository() -> ServingRepository:
    return ServingRepository(DATABASE_URL, connection_pool=DATABASE_POOL)


RepositoryDependency = Annotated[ServingRepository, Depends(repository)]


@app.get("/health/live")
def live() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready")
def ready(repo: RepositoryDependency) -> dict[str, str]:
    try:
        if repo.health():
            return {"status": "ready", "database": "healthy"}
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database unavailable") from exc
    raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database unavailable")


@app.get("/metrics", include_in_schema=False)
def metrics(repo: RepositoryDependency) -> Response:
    try:
        operational = repo.observability_snapshot()
    except Exception:
        operational = {
            "database_up": 0,
            "station_rows": 0,
            "active_alerts": 0,
            "critical_unacknowledged_alerts": 0,
            "reported_source_age_seconds": 0,
            "projection_age_seconds": 0,
        }
    return Response(
        content=api_metrics.render(operational),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


@app.get("/api/v1/alerts")
def alerts(
    repo: RepositoryDependency,
    lifecycle_status: Literal["OPEN", "ACKNOWLEDGED", "RESOLVED"] | None = None,
    recommended_action: Literal["DELIVER_BIKES", "REMOVE_BIKES", "INSPECT_STATION", "NONE"] | None = None,
    alert_type: str | None = None,
    station_id: str | None = None,
    active_only: bool = True,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    items = repo.list_alerts(
        lifecycle_status=lifecycle_status,
        recommended_action=recommended_action,
        alert_type=alert_type,
        station_id=station_id,
        active_only=active_only,
        limit=limit,
        offset=offset,
    )
    return {"items": items, "count": len(items), "limit": limit, "offset": offset}


@app.get("/api/v1/alerts/{alert_id}")
def alert(alert_id: str, repo: RepositoryDependency) -> dict:
    try:
        return repo.get_alert(alert_id)
    except ServingNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@app.get("/api/v1/alerts/{alert_id}/history")
def history(alert_id: str, repo: RepositoryDependency) -> dict:
    try:
        items = repo.alert_history(alert_id)
    except ServingNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return {"items": items, "count": len(items)}


@app.post("/api/v1/alerts/{alert_id}/acknowledge")
def acknowledge(alert_id: str, request: AcknowledgeRequest, repo: RepositoryDependency) -> dict:
    try:
        acknowledged_at = (
            request.acknowledged_at_utc.astimezone(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
            if request.acknowledged_at_utc
            else None
        )
        event = repo.acknowledge(
            alert_id=alert_id,
            idempotency_key=request.idempotency_key,
            requested_by=request.requested_by,
            acknowledged_at_utc=acknowledged_at,
        )
        return {"status": "ACKNOWLEDGED", "alert_event": event}
    except ServingNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ServingConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc


@app.get("/api/v1/stations")
def stations(
    repo: RepositoryDependency,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    items = repo.list_stations(limit=limit, offset=offset)
    return {"items": items, "count": len(items), "limit": limit, "offset": offset}


@app.get("/api/v1/stations/{station_id}")
def station(station_id: str, repo: RepositoryDependency) -> dict:
    try:
        return repo.get_station(station_id)
    except ServingNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
