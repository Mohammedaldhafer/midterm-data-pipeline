import argparse
import csv
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from pymongo import MongoClient


MONGO_URI = "mongodb://localhost:27017"
MONGO_DATABASE = "midterm_data_pipeline"
VALIDATED_COLLECTION = "orders_validated"


def get_mongo_client() -> MongoClient:
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000
    )
    client.admin.command("ping")
    return client


def parse_version(value: str) -> int:
    try:
        version = int(value)
    except (TypeError, ValueError):
        raise ValueError(
            f"Invalid version value: {value!r}"
        )

    if version < 1:
        raise ValueError(
            "version must be greater than or equal to 1."
        )

    return version


def load_delta(
    input_file: str,
    mongo_uri: str = MONGO_URI,
    database_name: str = MONGO_DATABASE,
    collection_name: str = VALIDATED_COLLECTION,
) -> Dict[str, Any]:

    input_path = Path(input_file)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Delta file does not exist: {input_path}"
        )

    if not input_path.is_file():
        raise ValueError(
            f"Delta path is not a file: {input_path}"
        )

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    count_inserted = 0
    count_updated = 0
    count_unchanged = 0
    count_older = 0
    count_processed = 0

    client = get_mongo_client()

    try:
        collection = client[
            database_name
        ][
            collection_name
        ]

        collection.create_index(
            [("id_order", 1)],
            unique=True,
            name="unique_id_order"
        )

        with input_path.open(
            "r",
            encoding="utf-8-sig",
            newline=""
        ) as file:

            reader = csv.DictReader(file)

            if not reader.fieldnames:
                raise ValueError(
                    "Delta file has no header."
                )

            required_fields = {
                "order_id",
                "version"
            }

            missing_fields = (
                required_fields
                - set(reader.fieldnames)
            )

            if missing_fields:
                raise ValueError(
                    "Delta file is missing required "
                    f"fields: {sorted(missing_fields)}"
                )

            for row in reader:

                count_processed += 1

                order_id = (
                    row.get("order_id") or ""
                ).strip()

                if not order_id:
                    raise ValueError(
                        "Delta contains a row "
                        "with an empty order_id."
                    )

                version = parse_version(
                    row.get("version")
                )

                existing = collection.find_one(
                    {
                        "id_order": order_id
                    }
                )

                new_document = dict(row)

                new_document["id_order"] = order_id
                new_document["version"] = version
                new_document["incremental_run_id"] = run_id
                new_document["incremental_loaded_at"] = started_at

                if existing is None:

                    collection.insert_one(
                        new_document
                    )

                    count_inserted += 1
                    continue

                existing_version = existing.get(
                    "version"
                )

                if existing_version is None:
                    existing_version = 0

                try:
                    existing_version = int(
                        existing_version
                    )
                except (TypeError, ValueError):
                    existing_version = 0

                if version > existing_version:

                    collection.update_one(
                        {
                            "id_order": order_id
                        },
                        {
                            "$set": new_document
                        }
                    )

                    count_updated += 1

                elif version == existing_version:

                    count_unchanged += 1

                else:

                    count_older += 1

        return {
            "id_run": run_id,
            "file_name": input_path.name,
            "count_processed": count_processed,
            "count_inserted": count_inserted,
            "count_updated": count_updated,
            "count_unchanged": count_unchanged,
            "count_older": count_older,
            "loaded_at": started_at.isoformat()
        }

    finally:
        client.close()


def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "Path B incremental Delta loader "
            "with version-based conflict handling."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Incremental Delta CSV file."
    )

    args = parser.parse_args()

    try:

        result = load_delta(
            args.input
        )

        print()
        print("=" * 75)
        print("INCREMENTAL DELTA LOAD COMPLETED")
        print("=" * 75)

        for key, value in result.items():
            print(
                f"{key:<22}: {value}"
            )

        print("=" * 75)

    except KeyboardInterrupt:

        print(
            "\nIncremental load interrupted."
        )
        sys.exit(1)

    except Exception as error:

        print()
        print("=" * 75)
        print("INCREMENTAL LOAD ERROR")
        print("=" * 75)
        print(
            f"{type(error).__name__}: {error}"
        )
        print(
            str(error)
        )
        print("=" * 75)

        sys.exit(1)


if __name__ == "__main__":
    main()
