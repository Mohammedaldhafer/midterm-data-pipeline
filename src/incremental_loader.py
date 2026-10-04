from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any

from pymongo import MongoClient

# Support both execution modes:
#   python .\\src\\incremental_loader.py
# and:
#   from src.incremental_loader import load_delta
try:
    from src.materialized_views import (
        DAILY_SALES_COLLECTION,
        TOP_PRODUCTS_COLLECTION,
        apply_incremental_change,
    )
except ImportError:
    from materialized_views import (
        DAILY_SALES_COLLECTION,
        TOP_PRODUCTS_COLLECTION,
        apply_incremental_change,
    )


MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "midterm_data_pipeline"
COLLECTION_NAME = "orders_validated"


def get_database():
    """Connect to MongoDB and return the project database."""

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )
    client.admin.command("ping")
    return client, client[DATABASE_NAME]


def parse_version(value: Any) -> int:
    """Convert a Delta version value to an integer."""

    try:
        return int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Invalid version value: {value!r}"
        ) from exc


def normalize_document(row: dict[str, Any]) -> dict[str, Any]:
    """
    Convert one CSV Delta row into a validated-order document.

    order_id is used as the business key and version controls conflicts.
    """

    order_id = str(
        row.get("order_id", "")
    ).strip()

    if not order_id:
        raise ValueError(
            "Delta row is missing required field: order_id"
        )

    version = parse_version(
        row.get("version")
    )

    document = dict(row)

    document["id_order"] = order_id
    document["version"] = version

    return document


def materialized_views_ready(db) -> bool:
    """
    Return True only when both materialized views already exist.

    Incremental changes are not applied to partially initialized views.
    """

    existing = set(
        db.list_collection_names()
    )

    return (
        DAILY_SALES_COLLECTION in existing
        and TOP_PRODUCTS_COLLECTION in existing
    )


def load_delta(
    input_file: str | Path,
    mongo_uri: str = MONGO_URI,
    database_name: str = DATABASE_NAME,
    collection_name: str = COLLECTION_NAME,
) -> dict[str, int | bool]:
    """
    Load a Delta CSV using version-aware Insert / Update / Unchanged / Older
    semantics and update materialized views incrementally when initialized.

    The same Delta replay is idempotent because equal versions are unchanged.
    Older versions are ignored and cannot overwrite newer records.
    """

    input_path = Path(input_file)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Delta file not found: {input_path}"
        )

    client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=5000,
    )

    try:
        db = client[database_name]
        collection = db[collection_name]

        collection.create_index(
            [("id_order", 1)],
            unique=True,
            name="unique_id_order",
        )

        mv_enabled = (
            apply_incremental_change is not None
            and materialized_views_ready(db)
        )

        count_processed = 0
        count_inserted = 0
        count_updated = 0
        count_unchanged = 0
        count_older = 0
        count_mv_applied = 0

        with input_path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as file:

            reader = csv.DictReader(file)

            if not reader.fieldnames:
                raise ValueError(
                    "Delta CSV has no header."
                )

            required = {
                "order_id",
                "version",
            }

            missing = required.difference(
                reader.fieldnames
            )

            if missing:
                raise ValueError(
                    "Delta CSV is missing required fields: "
                    + ", ".join(sorted(missing))
                )

            for row in reader:
                count_processed += 1

                new_document = normalize_document(row)

                order_id = new_document["id_order"]
                new_version = new_document["version"]

                existing = collection.find_one(
                    {"id_order": order_id}
                )

                if existing is None:
                    collection.insert_one(
                        new_document
                    )

                    count_inserted += 1

                    if mv_enabled:
                        result = apply_incremental_change(
                            db,
                            None,
                            new_document,
                        )
                        if result.get("applied"):
                            count_mv_applied += 1

                    continue

                existing_version = parse_version(
                    existing.get("version", 0)
                )

                if new_version == existing_version:
                    count_unchanged += 1
                    continue

                if new_version < existing_version:
                    count_older += 1
                    continue

                collection.replace_one(
                    {"id_order": order_id},
                    new_document,
                    upsert=False,
                )

                count_updated += 1

                if mv_enabled:
                    result = apply_incremental_change(
                        db,
                        existing,
                        new_document,
                    )

                    if result.get("applied"):
                        count_mv_applied += 1

        return {
            "count_processed": count_processed,
            "count_inserted": count_inserted,
            "count_updated": count_updated,
            "count_unchanged": count_unchanged,
            "count_older": count_older,
            "count_mv_applied": count_mv_applied,
            "materialized_views_enabled": mv_enabled,
        }

    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Version-aware incremental Delta loader "
            "with optional incremental materialized-view refresh."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to the Delta CSV file.",
    )

    args = parser.parse_args()

    result = load_delta(
        args.input
    )

    print("=" * 80)
    print("INCREMENTAL DELTA LOAD")
    print("=" * 80)

    for key, value in result.items():
        label = key.replace("_", " ")
        print(
            f"{label:<30}: {value}"
        )


if __name__ == "__main__":
    main()
