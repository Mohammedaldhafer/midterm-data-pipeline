from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from pymongo import MongoClient

try:
    from .aggregations import get_collection as get_aggregation_collection
    from .aggregations import REPORTS, run_report
    from .file_router import select_engine
    from .jobs import JOB_SCHEDULES, JOBS, run_job
    from .main import run_pipeline
    from .materialized_views import (
        DAILY_SALES_COLLECTION,
        TOP_PRODUCTS_COLLECTION,
        create_mv_indexes,
    )
    from .queries import (
        QUERY_INDEXES,
        find_dynamic_parameters,
        query_city_orders_by_date,
        query_customer_orders,
        query_orders_by_status,
        query_orders_by_status_and_city,
        query_recent_orders_by_city,
    )
except ImportError:
    from aggregations import get_collection as get_aggregation_collection
    from aggregations import REPORTS, run_report
    from file_router import select_engine
    from jobs import JOB_SCHEDULES, JOBS, run_job
    from main import run_pipeline
    from materialized_views import (
        DAILY_SALES_COLLECTION,
        TOP_PRODUCTS_COLLECTION,
        create_mv_indexes,
    )
    from queries import (
        QUERY_INDEXES,
        find_dynamic_parameters,
        query_city_orders_by_date,
        query_customer_orders,
        query_orders_by_status,
        query_orders_by_status_and_city,
        query_recent_orders_by_city,
    )


MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://localhost:27017",
)
DATABASE_NAME = os.getenv(
    "MONGO_DATABASE",
    "midterm_data_pipeline",
)
VALIDATED_COLLECTION = "orders_validated"

app = FastAPI(
    title="Midterm Data Pipeline API",
    version="1.0.0",
    description=(
        "Unified FastAPI interface for ingestion, practical queries, "
        "aggregation reports, materialized-view refresh, and scheduled jobs."
    ),
)


def get_db():
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )
    client.admin.command("ping")
    return client, client[DATABASE_NAME]


def _query_defaults(collection) -> dict[str, str]:
    return find_dynamic_parameters(collection)


def _clean_limit(limit: int) -> int:
    if limit < 1 or limit > 100:
        raise HTTPException(
            status_code=400,
            detail="limit must be between 1 and 100.",
        )
    return limit


@app.get("/health")
def health() -> dict[str, Any]:
    try:
        client, db = get_db()
        try:
            ping = client.admin.command("ping")
            views = set(db.list_collection_names())
            return {
                "status": "ok",
                "mongodb": ping.get("ok") == 1,
                "database": DATABASE_NAME,
                "materialized_views": {
                    "daily_sales_summary": DAILY_SALES_COLLECTION in views,
                    "top_products_summary": TOP_PRODUCTS_COLLECTION in views,
                },
            }
        finally:
            client.close()
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"MongoDB unavailable: {type(exc).__name__}: {exc}",
        ) from exc


@app.post("/ingest")
def ingest(
    file: UploadFile = File(...),
    batch_size: int = Query(5000, ge=1),
) -> dict[str, Any]:
    suffix = Path(file.filename or "upload.csv").suffix or ".csv"

    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix="api_ingest_",
            suffix=suffix,
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)

            while True:
                chunk = file.file.read(1024 * 1024)
                if not chunk:
                    break
                temp_file.write(chunk)

        decision = select_engine(str(temp_path))
        result = run_pipeline(
            input_file=str(temp_path),
            batch_size=batch_size,
        )

        result["api_router_decision"] = decision
        return result
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Ingestion failed: {type(exc).__name__}: {exc}",
        ) from exc
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


@app.post("/indexes")
def create_indexes() -> dict[str, Any]:
    client, db = get_db()

    created: dict[str, list[str]] = {
        "query_indexes": [],
        "materialized_view_indexes": [],
    }

    try:
        validated = db[VALIDATED_COLLECTION]

        for name, keys in QUERY_INDEXES.items():
            validated.create_index(keys, name=name)
            created["query_indexes"].append(name)

        create_mv_indexes(db)
        created["materialized_view_indexes"] = [
            "unique_day",
            "unique_product",
            "unique_mv_order",
        ]

        return {
            "status": "ok",
            "indexes": created,
        }
    finally:
        client.close()


QUERY_METADATA = {
    "recent_orders_by_city": {
        "description": "Recent orders for a city, sorted by order date.",
        "parameters": ["city", "limit"],
    },
    "city_orders_by_date": {
        "description": "Orders for a city inside a date range.",
        "parameters": ["city", "start_date", "end_date", "limit"],
    },
    "customer_orders": {
        "description": "Customer orders inside a date range.",
        "parameters": [
            "customer_id",
            "start_date",
            "end_date",
            "limit",
        ],
    },
    "orders_by_status": {
        "description": "Orders with a status inside a date range.",
        "parameters": ["status", "start_date", "end_date", "limit"],
    },
    "orders_by_status_and_city": {
        "description": "Orders matching status, city, and date range.",
        "parameters": [
            "status",
            "city",
            "start_date",
            "end_date",
            "limit",
        ],
    },
}


@app.get("/queries")
def list_queries() -> dict[str, Any]:
    return {
        "count": len(QUERY_METADATA),
        "queries": QUERY_METADATA,
    }


@app.get("/queries/{name}")
def execute_query(
    name: str,
    city: str | None = None,
    customer_id: str | None = None,
    status: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = Query(10, ge=1, le=100),
) -> dict[str, Any]:
    if name not in QUERY_METADATA:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Unknown query '{name}'. "
                f"Available: {', '.join(QUERY_METADATA)}"
            ),
        )

    limit = _clean_limit(limit)

    client, collection = get_aggregation_collection()
    try:
        params = _query_defaults(collection)

        city = city or params["city"]
        customer_id = customer_id or params["customer_id"]
        status = status or params["status"]
        start_date = start_date or params["start_date"]
        end_date = end_date or params["end_date"]

        if name == "recent_orders_by_city":
            rows = query_recent_orders_by_city(
                collection,
                city=city,
                limit=limit,
            )
        elif name == "city_orders_by_date":
            rows = query_city_orders_by_date(
                collection,
                city=city,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
            )
        elif name == "customer_orders":
            rows = query_customer_orders(
                collection,
                customer_id=customer_id,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
            )
        elif name == "orders_by_status":
            rows = query_orders_by_status(
                collection,
                status=status,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
            )
        else:
            rows = query_orders_by_status_and_city(
                collection,
                status=status,
                city=city,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
            )

        return {
            "name": name,
            "parameters": {
                "city": city,
                "customer_id": customer_id,
                "status": status,
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
            },
            "count": len(rows),
            "rows": rows,
        }
    finally:
        client.close()


@app.get("/aggregations")
def list_aggregations() -> dict[str, Any]:
    return {
        "count": len(REPORTS),
        "aggregations": [
            {
                "name": name,
                "description": getattr(
                    function,
                    "__doc__",
                    "",
                ).strip(),
            }
            for name, function in REPORTS.items()
        ],
    }


@app.get("/aggregations/{name}")
def execute_aggregation(
    name: str,
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    if name not in REPORTS:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Unknown aggregation '{name}'. "
                f"Available: {', '.join(REPORTS)}"
            ),
        )

    limit = _clean_limit(limit)

    client, collection = get_aggregation_collection()
    try:
        rows = run_report(
            name,
            collection,
            limit=limit,
        )
        return {
            "name": name,
            "count": len(rows),
            "rows": rows,
        }
    finally:
        client.close()


@app.post("/refresh-mv")
def refresh_materialized_views() -> dict[str, Any]:
    """
    Refresh materialized views through the existing incremental Delta job.

    This endpoint intentionally does not rebuild the views from the full
    orders_validated collection. A full initialization is a one-time CLI
    operation; ongoing refresh is incremental.
    """
    result = run_job("refresh_delta")
    return {
        "status": result.get("status"),
        "job": result,
        "mode": "incremental",
    }


@app.get("/jobs")
def list_jobs() -> dict[str, Any]:
    return {
        "count": len(JOBS),
        "jobs": [
            {
                "name": name,
                "schedule": JOB_SCHEDULES.get(name),
            }
            for name in JOBS
        ],
    }


@app.post("/jobs/{name}/run")
def execute_job(name: str) -> dict[str, Any]:
    if name not in JOBS:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Unknown job '{name}'. "
                f"Available: {', '.join(JOBS)}"
            ),
        )

    try:
        return run_job(name)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Job failed: {type(exc).__name__}: {exc}",
        ) from exc
