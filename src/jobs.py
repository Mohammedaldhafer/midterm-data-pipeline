from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from dotenv import load_dotenv
from pymongo import MongoClient

try:
    from src.aggregations import get_collection, run_report
    from src.incremental_loader import load_delta
    from src.materialized_views import (
        DAILY_SALES_COLLECTION,
        TOP_PRODUCTS_COLLECTION,
        create_mv_indexes,
    )
except ImportError:
    from aggregations import get_collection, run_report
    from incremental_loader import load_delta
    from materialized_views import (
        DAILY_SALES_COLLECTION,
        TOP_PRODUCTS_COLLECTION,
        create_mv_indexes,
    )


load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DATABASE_NAME = os.getenv("MONGO_DATABASE", "midterm_data_pipeline")
LOG_DIR = Path(os.getenv("JOBS_LOG_DIR", "logs"))
JOB_LOG_FILE = LOG_DIR / "jobs.log"

DELTA_INPUT_DIR = Path(
    os.getenv("DELTA_INPUT_DIR", "data/delta/incoming")
)
DELTA_PROCESSED_DIR = Path(
    os.getenv("DELTA_PROCESSED_DIR", "data/delta/processed")
)

JOBS_COLLECTION = "job_runs"


LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(JOB_LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("scheduled_jobs")


JOB_SCHEDULES = {
    "refresh_delta": "Daily at 02:00",
    "aggregation_snapshot": "Daily at 03:00",
}


def get_database():
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )
    client.admin.command("ping")
    return client, client[DATABASE_NAME]


def _save_job_run(
    job_name: str,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    result: dict[str, Any] | None = None,
    error: str | None = None,
):
    client, db = get_database()
    try:
        db[JOBS_COLLECTION].create_index(
            [("job_name", 1), ("started_at", -1)],
            name="idx_job_started",
        )
        db[JOBS_COLLECTION].insert_one(
            {
                "job_name": job_name,
                "started_at": started_at,
                "finished_at": finished_at,
                "status": status,
                "result": result or {},
                "error": error,
            }
        )
    finally:
        client.close()


def _execute_job(
    job_name: str,
    function: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    started_at = datetime.now()
    logger.info("JOB START | %s", job_name)

    try:
        result = function()
        finished_at = datetime.now()
        _save_job_run(
            job_name,
            started_at,
            finished_at,
            "success",
            result=result,
        )
        logger.info("JOB END | %s | success | %s", job_name, result)
        return {
            "job_name": job_name,
            "status": "success",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "result": result,
        }
    except Exception as exc:
        finished_at = datetime.now()
        error = f"{type(exc).__name__}: {exc}"
        try:
            _save_job_run(
                job_name,
                started_at,
                finished_at,
                "failure",
                error=error,
            )
        except Exception as log_exc:
            logger.error("JOB LOG FAILURE | %s | %s", job_name, log_exc)

        logger.exception("JOB END | %s | failure", job_name)
        return {
            "job_name": job_name,
            "status": "failure",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "error": error,
        }


def refresh_delta_job() -> dict[str, Any]:
    """
    Process pending Delta CSV files incrementally.

    The job reuses the existing version-aware incremental_loader, which also
    updates materialized views incrementally when those views are initialized.
    Successfully processed files are moved to the processed directory.
    """

    DELTA_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    DELTA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    client, db = get_database()
    try:
        existing = set(db.list_collection_names())
        views_ready = (
            DAILY_SALES_COLLECTION in existing
            and TOP_PRODUCTS_COLLECTION in existing
        )
    finally:
        client.close()

    if not views_ready:
        raise RuntimeError(
            "Materialized views are not initialized. "
            "Initialize them once before running refresh_delta."
        )

    files = sorted(DELTA_INPUT_DIR.glob("*.csv"))
    processed_files: list[str] = []
    results: list[dict[str, Any]] = []

    for input_file in files:
        result = load_delta(input_file)
        processed_path = DELTA_PROCESSED_DIR / input_file.name

        if processed_path.exists():
            stem = processed_path.stem
            suffix = processed_path.suffix
            processed_path = (
                DELTA_PROCESSED_DIR
                / f"{stem}_{datetime.now().strftime('%Y%m%d%H%M%S')}{suffix}"
            )

        shutil.move(str(input_file), str(processed_path))
        processed_files.append(processed_path.name)
        results.append(
            {
                "file": input_file.name,
                "load_result": result,
            }
        )

    return {
        "files_found": len(files),
        "files_processed": processed_files,
        "results": results,
        "mode": "incremental_delta",
    }


def aggregation_snapshot_job() -> dict[str, Any]:
    """Run lightweight named aggregation reports and persist a snapshot."""

    client, collection = get_collection()
    try:
        report_names = [
            "sales_by_city",
            "orders_by_status",
            "sales_by_month",
        ]

        snapshot: dict[str, Any] = {}
        for name in report_names:
            rows = run_report(name, collection, limit=20)
            snapshot[name] = {
                "row_count": len(rows),
                "rows": rows,
            }

        db = collection.database
        document = {
            "created_at": datetime.now(),
            "reports": snapshot,
        }
        insert_result = db["aggregation_job_snapshots"].insert_one(document)

        return {
            "reports": report_names,
            "snapshot_id": str(insert_result.inserted_id),
            "mode": "aggregation_snapshot",
        }
    finally:
        client.close()


JOBS: dict[str, Callable[[], dict[str, Any]]] = {
    "refresh_delta": refresh_delta_job,
    "aggregation_snapshot": aggregation_snapshot_job,
}


def run_job(name: str) -> dict[str, Any]:
    if name not in JOBS:
        available = ", ".join(JOBS)
        raise ValueError(
            f"Unknown job '{name}'. Available jobs: {available}"
        )

    return _execute_job(name, JOBS[name])


def list_jobs() -> list[dict[str, str]]:
    return [
        {
            "name": name,
            "schedule": JOB_SCHEDULES[name],
        }
        for name in JOBS
    ]


def run_scheduler() -> None:
    """Run the two fixed daily schedules in a lightweight stdlib scheduler."""

    last_run_date: dict[str, str | None] = {
        name: None for name in JOBS
    }

    logger.info("SCHEDULER START | schedules=%s", JOB_SCHEDULES)

    while True:
        now = datetime.now()
        date_key = now.strftime("%Y-%m-%d")
        hhmm = now.strftime("%H:%M")

        due_jobs = {
            "02:00": "refresh_delta",
            "03:00": "aggregation_snapshot",
        }

        job_name = due_jobs.get(hhmm)
        if (
            job_name
            and last_run_date[job_name] != date_key
        ):
            run_job(job_name)
            last_run_date[job_name] = date_key

        time.sleep(20)


def main():
    parser = argparse.ArgumentParser(
        description="Scheduled jobs for the final project."
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List jobs and fixed schedules.",
    )
    parser.add_argument(
        "--run",
        choices=list(JOBS),
        help="Run one job manually now.",
    )
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Start the fixed daily scheduler.",
    )

    args = parser.parse_args()

    if args.list:
        print(json.dumps(list_jobs(), ensure_ascii=False, indent=2))
        return

    if args.run:
        result = run_job(args.run)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return

    if args.schedule:
        run_scheduler()
        return

    parser.error("Choose one of --list, --run, or --schedule")


if __name__ == "__main__":
    main()
