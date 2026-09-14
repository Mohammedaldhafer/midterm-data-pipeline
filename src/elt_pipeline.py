import json
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from pymongo import MongoClient

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)

from quality_rules import apply_quality_rules
from upsert_manager import upsert_documents

ELT_BATCH_SIZE = 5000
MONGO_CURSOR_BATCH_SIZE = 5000

ERROR_CODES = {
    "ID_ORDER_MISSING":
        "Order ID is missing and cannot be safely inferred.",
    "ID_CUSTOMER_MISSING":
        "Customer ID is missing and cannot be safely inferred.",
    "DATE_IMPOSSIBLE_INVALID":
        "Date is invalid or impossible.",
    "JSON_ITEMS_CORRUPTED":
        "Items JSON is missing, incomplete, or invalid.",
    "ITEMS_EMPTY":
        "Order contains no items.",
    "PRICE_UNKNOWN":
        "Original price is missing or cannot be safely inferred.",
    "VALUE_NEGATIVE_AMBIGUOUS":
        "Negative quantity or amount has an ambiguous meaning.",
    "ERRORS_CONFLICTING_MULTIPLE":
        "Multiple essential errors prevent safe correction.",
}


def get_mongo_client():

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000,
        connectTimeoutMS=20000,
        socketTimeoutMS=120000,
        retryWrites=True
    )

    client.admin.command("ping")

    return client


def find_field(
    record: Dict[str, Any],
    aliases: List[str]
):

    normalized = {
        str(key).strip().lower(): key
        for key in record.keys()
    }

    for alias in aliases:

        alias = alias.strip().lower()

        if alias in normalized:
            return normalized[alias]

    return None


def get_value(
    record: Dict[str, Any],
    aliases: List[str]
):

    field = find_field(
        record,
        aliases
    )

    if field is None:
        return None

    return record.get(field)


def is_empty(value):

    if value is None:
        return True

    return str(value).strip() == ""


def parse_items_json(items_value):

    if is_empty(items_value):
        return None

    if isinstance(items_value, list):
        return items_value

    try:

        parsed = json.loads(
            str(items_value)
        )

        if isinstance(parsed, list):
            return parsed

    except (
        json.JSONDecodeError,
        TypeError,
        ValueError
    ):
        pass

    return None


def has_valid_item_price(items_value):

    parsed = parse_items_json(
        items_value
    )

    if not parsed:
        return False

    for item in parsed:

        if not isinstance(item, dict):
            continue

        for field_name in (
            "unit_price",
            "price",
            "item_price"
        ):

            value = item.get(
                field_name
            )

            if is_empty(value):
                continue

            try:

                price = float(value)

                if price >= 0:
                    return True

            except (
                ValueError,
                TypeError
            ):
                continue

    return False


def validate_record(
    cleaned_record: Dict[str, Any]
) -> Tuple[bool, List[str], List[str]]:

    errors = []
    details = []

    order_id = get_value(
        cleaned_record,
        [
            "id_order",
            "order_id",
            "orderid",
        ]
    )

    if is_empty(order_id):

        errors.append(
            "ID_ORDER_MISSING"
        )

        details.append(
            ERROR_CODES[
                "ID_ORDER_MISSING"
            ]
        )

    customer_id = get_value(
        cleaned_record,
        [
            "id_customer",
            "customer_id",
            "customerid",
        ]
    )

    if is_empty(customer_id):

        errors.append(
            "ID_CUSTOMER_MISSING"
        )

        details.append(
            ERROR_CODES[
                "ID_CUSTOMER_MISSING"
            ]
        )

    date_value = get_value(
        cleaned_record,
        [
            "order_date",
            "date",
            "created_at",
            "at_order",
        ]
    )

    if date_value is not None:

        date_text = str(
            date_value
        ).strip()

        if date_text:

            try:

                normalized_date = (
                    date_text
                    .replace("Z", "+00:00")
                )

                try:

                    parsed_date = (
                        datetime.fromisoformat(
                            normalized_date
                        )
                    )

                except ValueError:

                    parsed_date = (
                        datetime.strptime(
                            date_text[:10],
                            "%Y-%m-%d"
                        )
                    )

                if not (
                    1900 <= parsed_date.year <= 2100
                ):

                    raise ValueError

            except (
                ValueError,
                TypeError,
                OverflowError
            ):

                errors.append(
                    "DATE_IMPOSSIBLE_INVALID"
                )

                details.append(
                    ERROR_CODES[
                        "DATE_IMPOSSIBLE_INVALID"
                    ]
                )

    items_value = get_value(
        cleaned_record,
        [
            "items",
            "order_items",
            "items_json",
        ]
    )

    parsed_items = None

    if items_value is not None:

        if is_empty(items_value):

            errors.append(
                "ITEMS_EMPTY"
            )

            details.append(
                ERROR_CODES[
                    "ITEMS_EMPTY"
                ]
            )

        else:

            parsed_items = parse_items_json(
                items_value
            )

            if parsed_items is None:

                errors.append(
                    "JSON_ITEMS_CORRUPTED"
                )

                details.append(
                    ERROR_CODES[
                        "JSON_ITEMS_CORRUPTED"
                    ]
                )

            elif len(parsed_items) == 0:

                errors.append(
                    "ITEMS_EMPTY"
                )

                details.append(
                    ERROR_CODES[
                        "ITEMS_EMPTY"
                    ]
                )

    price_value = get_value(
        cleaned_record,
        [
            "price",
            "unit_price",
            "item_price",
        ]
    )

    if is_empty(price_value):

        if not has_valid_item_price(
            items_value
        ):

            errors.append(
                "PRICE_UNKNOWN"
            )

            details.append(
                ERROR_CODES[
                    "PRICE_UNKNOWN"
                ]
            )

    numeric_fields = [
        "price",
        "unit_price",
        "item_price",
        "quantity",
        "amount",
        "total",
        "order_total",
        "total_amount",
    ]

    for field_name in numeric_fields:

        field = find_field(
            cleaned_record,
            [field_name]
        )

        if field is None:
            continue

        value = cleaned_record.get(
            field
        )

        if is_empty(value):
            continue

        try:

            if float(value) < 0:

                errors.append(
                    "VALUE_NEGATIVE_AMBIGUOUS"
                )

                details.append(
                    ERROR_CODES[
                        "VALUE_NEGATIVE_AMBIGUOUS"
                    ]
                )

                break

        except (
            ValueError,
            TypeError
        ):
            pass

    if parsed_items:

        for item in parsed_items:

            if not isinstance(item, dict):
                continue

            for field_name in (
                "qty",
                "quantity",
                "price",
                "unit_price",
                "item_price",
                "amount",
                "total",
            ):

                value = item.get(
                    field_name
                )

                if is_empty(value):
                    continue

                try:

                    if float(value) < 0:

                        errors.append(
                            "VALUE_NEGATIVE_AMBIGUOUS"
                        )

                        details.append(
                            ERROR_CODES[
                                "VALUE_NEGATIVE_AMBIGUOUS"
                            ]
                        )

                        break

                except (
                    ValueError,
                    TypeError
                ):
                    continue

            if (
                "VALUE_NEGATIVE_AMBIGUOUS"
                in errors
            ):
                break

    unique_errors = []
    unique_details = []

    for code, detail in zip(
        errors,
        details
    ):

        if code not in unique_errors:

            unique_errors.append(
                code
            )

            unique_details.append(
                detail
            )

    errors = unique_errors
    details = unique_details

    if len(errors) >= 3:

        errors.append(
            "ERRORS_CONFLICTING_MULTIPLE"
        )

        details.append(
            ERROR_CODES[
                "ERRORS_CONFLICTING_MULTIPLE"
            ]
        )

    return (
        len(errors) == 0,
        errors,
        details,
    )


def classify_record(
    raw_document: Dict[str, Any]
):

    raw_record = dict(
        raw_document.get(
            "record_raw",
            {}
        )
    )

    cleaned = apply_quality_rules(
        raw_record
    )

    (
        valid,
        errors,
        details
    ) = validate_record(
        cleaned
    )

    corrections = cleaned.get(
        "corrections",
        []
    )

    if not valid:

        status = "quarantined"

    elif corrections:

        status = "corrected"

    else:

        status = "valid"

    result = dict(
        raw_document
    )

    result.update(
        cleaned
    )

    result["record_raw"] = raw_record
    result["quality_status"] = status
    result["codes_error"] = errors
    result["details_error"] = details

    return result


def process_batch(
    raw_documents,
    quarantine_collection,
):

    valid_documents = []
    quarantine_documents = []

    error_counter = Counter()

    count_valid = 0
    count_corrected = 0
    count_quarantine = 0

    for raw_document in raw_documents:

        processed = classify_record(
            raw_document
        )

        status = processed[
            "quality_status"
        ]

        if status == "valid":

            count_valid += 1

            valid_documents.append(
                processed
            )

        elif status == "corrected":

            count_corrected += 1

            valid_documents.append(
                processed
            )

        else:

            count_quarantine += 1

            quarantine_documents.append(
                processed
            )

            for code in processed[
                "codes_error"
            ]:

                error_counter[code] += 1

    upsert_result = upsert_documents(
        valid_documents
    )

    quarantine_inserted = 0

    if quarantine_documents:

        result = quarantine_collection.insert_many(
            quarantine_documents,
            ordered=False
        )

        quarantine_inserted = len(
            result.inserted_ids
        )

    return {
        "count_valid":
            count_valid,

        "count_corrected":
            count_corrected,

        "count_quarantine":
            count_quarantine,

        "count_quarantine_inserted":
            quarantine_inserted,

        "counts_case_error":
            dict(error_counter),

        "count_inserted":
            upsert_result[
                "count_inserted"
            ],

        "count_updated":
            upsert_result[
                "count_updated"
            ],

        "count_unchanged":
            upsert_result[
                "count_unchanged"
            ],
    }


def get_resume_row(
    validated_collection,
    quarantine_collection,
    id_run: str
):

    validated = validated_collection.find_one(
        {
            "id_run": id_run
        },
        sort=[
            (
                "number_row_source",
                -1
            )
        ]
    )

    quarantine = quarantine_collection.find_one(
        {
            "id_run": id_run
        },
        sort=[
            (
                "number_row_source",
                -1
            )
        ]
    )

    validated_row = (
        validated.get(
            "number_row_source"
        )
        if validated
        else None
    )

    quarantine_row = (
        quarantine.get(
            "number_row_source"
        )
        if quarantine
        else None
    )

    rows = [
        row
        for row in (
            validated_row,
            quarantine_row
        )
        if isinstance(
            row,
            (int, float)
        )
    ]

    return max(rows) if rows else 0


def process_run(
    id_run: str,
    batch_size: int = ELT_BATCH_SIZE,
    resume: bool = False
):

    if batch_size <= 0:

        raise ValueError(
            "batch_size must be greater than zero."
        )

    client = get_mongo_client()

    start_time = time.perf_counter()

    try:

        db = client[
            MONGO_DATABASE
        ]

        raw_collection = db[
            RAW_COLLECTION
        ]

        validated_collection = db[
            VALIDATED_COLLECTION
        ]

        quarantine_collection = db[
            QUARANTINE_COLLECTION
        ]

        resume_row = 0

        if resume:

            resume_row = get_resume_row(
                validated_collection,
                quarantine_collection,
                id_run
            )

            print()
            print("=" * 75)
            print("RESUME MODE")
            print("=" * 75)

            print(
                f"Resume after source row: "
                f"{resume_row:,}"
            )

            print("=" * 75)

        else:

            old_quarantine = quarantine_collection.delete_many(
                {
                    "id_run": id_run
                }
            )

            print()
            print("=" * 75)
            print("OLD QUARANTINE CLEANUP")
            print("=" * 75)

            print(
                f"Deleted old quarantine records: "
                f"{old_quarantine.deleted_count:,}"
            )

            print("=" * 75)

        raw_query = {
            "id_run": id_run
        }

        if resume:

            raw_query[
                "number_row_source"
            ] = {
                "$gt": resume_row
            }

        raw_records = raw_collection.find(
            raw_query,
            no_cursor_timeout=True
        ).sort(
            "number_row_source",
            1
        ).batch_size(
            MONGO_CURSOR_BATCH_SIZE
        )

        count_raw = 0
        count_valid = 0
        count_corrected = 0
        count_quarantine = 0

        count_inserted = 0
        count_updated = 0
        count_unchanged = 0

        error_counter = Counter()

        batch_number = 0
        batch = []

        print()
        print("=" * 75)
        print("ELT PIPELINE STARTED")
        print("=" * 75)

        print(
            f"Run ID              : {id_run}"
        )

        print(
            f"Batch size           : {batch_size:,}"
        )

        print(
            f"Mongo cursor batch   : "
            f"{MONGO_CURSOR_BATCH_SIZE:,}"
        )

        print(
            f"Resume mode          : "
            f"{'ON' if resume else 'OFF'}"
        )

        if resume:

            print(
                f"Resume after row     : "
                f"{resume_row:,}"
            )

        print("=" * 75)

        for raw_document in raw_records:

            batch.append(
                raw_document
            )

            count_raw += 1

            if len(batch) >= batch_size:

                batch_number += 1

                batch_start = time.perf_counter()

                result = process_batch(
                    batch,
                    quarantine_collection
                )

                elapsed_batch = (
                    time.perf_counter()
                    - batch_start
                )

                count_valid += result[
                    "count_valid"
                ]

                count_corrected += result[
                    "count_corrected"
                ]

                count_quarantine += result[
                    "count_quarantine"
                ]

                count_inserted += result[
                    "count_inserted"
                ]

                count_updated += result[
                    "count_updated"
                ]

                count_unchanged += result[
                    "count_unchanged"
                ]

                error_counter.update(
                    result[
                        "counts_case_error"
                    ]
                )

                elapsed_total = (
                    time.perf_counter()
                    - start_time
                )

                rate = (
                    count_raw / elapsed_total
                    if elapsed_total > 0
                    else 0
                )

                print(
                    f"Batch #{batch_number:<7} | "
                    f"Processed: {len(batch):<7,} | "
                    f"Raw total: {count_raw:<12,} | "
                    f"Valid: {count_valid:<9,} | "
                    f"Corrected: {count_corrected:<9,} | "
                    f"Quarantine: {count_quarantine:<9,} | "
                    f"Time: {elapsed_batch:.2f}s | "
                    f"Rate: {rate:,.2f}/sec"
                )

                batch.clear()

        if batch:

            batch_number += 1

            batch_count = len(
                batch
            )

            batch_start = time.perf_counter()

            result = process_batch(
                batch,
                quarantine_collection
            )

            elapsed_batch = (
                time.perf_counter()
                - batch_start
            )

            count_valid += result[
                "count_valid"
            ]

            count_corrected += result[
                "count_corrected"
            ]

            count_quarantine += result[
                "count_quarantine"
            ]

            count_inserted += result[
                "count_inserted"
            ]

            count_updated += result[
                "count_updated"
            ]

            count_unchanged += result[
                "count_unchanged"
            ]

            error_counter.update(
                result[
                    "counts_case_error"
                ]
            )

            elapsed_total = (
                time.perf_counter()
                - start_time
            )

            rate = (
                count_raw / elapsed_total
                if elapsed_total > 0
                else 0
            )

            print(
                f"Batch #{batch_number:<7} | "
                f"Processed: {batch_count:<7,} | "
                f"Raw total: {count_raw:<12,} | "
                f"Valid: {count_valid:<9,} | "
                f"Corrected: {count_corrected:<9,} | "
                f"Quarantine: {count_quarantine:<9,} | "
                f"Time: {elapsed_batch:.2f}s | "
                f"Rate: {rate:,.2f}/sec"
            )

            batch.clear()

        raw_records.close()

        elapsed = (
            time.perf_counter()
            - start_time
        )

        throughput = (
            count_raw / elapsed
            if elapsed > 0
            else 0
        )

        processed = (
    count_valid
    +
    count_corrected
    +
    count_quarantine
)

        consistency = (
            processed == count_raw
        )

        print()
        print("=" * 75)
        print("ELT PIPELINE COMPLETED")
        print("=" * 75)

        print(
            f"Raw                 : "
            f"{count_raw:,}"
        )

        print(
            f"Valid               : "
            f"{count_valid:,}"
        )

        print(
            f"Corrected           : "
            f"{count_corrected:,}"
        )

        print(
            f"Quarantine          : "
            f"{count_quarantine:,}"
        )

        print(
            f"Inserted            : "
            f"{count_inserted:,}"
        )

        print(
            f"Updated             : "
            f"{count_updated:,}"
        )

        print(
            f"Unchanged           : "
            f"{count_unchanged:,}"
        )

        print(
            f"Batches             : "
            f"{batch_number:,}"
        )

        print(
            f"Elapsed time        : "
            f"{elapsed:.3f} seconds"
        )

        print(
            f"Throughput          : "
            f"{throughput:,.2f} records/sec"
        )

        print(
            f"Consistency         : "
            f"{'PASS' if consistency else 'FAIL'}"
        )

        print("=" * 75)

        print()
        print("ERROR SUMMARY")
        print("=" * 75)

        for code, count in error_counter.most_common():

            print(
                f"{code:<35} : "
                f"{count:,}"
            )

        print("=" * 75)

        return {
            "id_run": id_run,
            "raw": count_raw,
            "valid": count_valid,
            "corrected": count_corrected,
            "quarantine": count_quarantine,
            "inserted": count_inserted,
            "updated": count_updated,
            "unchanged": count_unchanged,
            "batches": batch_number,
            "elapsed": round(
                elapsed,
                3
            ),
            "throughput": round(
                throughput,
                2
            ),
            "consistency": consistency,
            "errors": dict(
                error_counter
            )
        }

    finally:

        client.close()


def main():

    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--id-run",
        required=True
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=ELT_BATCH_SIZE
    )

    parser.add_argument(
        "--resume",
        action="store_true"
    )

    args = parser.parse_args()

    try:

        process_run(
            args.id_run,
            args.batch_size,
            args.resume
        )

    except Exception as error:

        print()
        print("=" * 75)
        print("ERROR")
        print("=" * 75)

        print(
            f"{type(error).__name__}: {error}"
        )

        print("=" * 75)

        sys.exit(1)


if __name__ == "__main__":
    main()