import csv
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


# ============================================================
# Project Root
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================
# Project Configuration
# ============================================================

from config.settings import (
    BATCH_SIZE,
    MONGO_URI,
    MONGO_DATABASE,
    RAW_COLLECTION,
)


# ============================================================
# MongoDB
# ============================================================

from pymongo import MongoClient


def get_mongo_client():
    """
    Create a MongoDB client and test the connection.
    """

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000
    )

    client.admin.command("ping")

    return client


# ============================================================
# Utility
# ============================================================

def generate_run_id():
    """
    Generate a unique ID for the current pipeline run.
    """

    return str(uuid.uuid4())


def get_current_utc_time():
    """
    Return the current UTC time as a datetime object.

    Important:
    MongoDB schema expects `at_ingested` to be a BSON date.
    Therefore, do NOT convert the datetime to an ISO string.
    """

    return datetime.now(timezone.utc)


# ============================================================
# Batch Loader
# ============================================================

def load_csv_to_raw(
    input_file: str,
    batch_size: int = BATCH_SIZE
):
    """
    Stream a CSV file and load all records into orders_raw.

    Important:
    - The entire CSV is NOT loaded into memory.
    - No cleaning is performed.
    - Every record is stored in Raw.
    """

    input_path = Path(input_file)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file does not exist: {input_path}"
        )

    if not input_path.is_file():
        raise ValueError(
            f"Input path is not a file: {input_path}"
        )

    if batch_size <= 0:
        raise ValueError(
            "batch_size must be greater than zero."
        )

    # --------------------------------------------------------
    # Run information
    # --------------------------------------------------------

    id_run = generate_run_id()

    file_source = str(input_path.resolve())

    total_rows = 0
    batch_number = 0

    start_time = time.perf_counter()

    # --------------------------------------------------------
    # MongoDB connection
    # --------------------------------------------------------

    client = get_mongo_client()

    try:

        db = client[MONGO_DATABASE]
        collection = db[RAW_COLLECTION]

        print("=" * 70)
        print("PYTHON BATCH RAW LOADER")
        print("=" * 70)

        print(f"Run ID          : {id_run}")
        print(f"Input file      : {file_source}")
        print(f"Batch size      : {batch_size}")
        print(f"MongoDB         : {MONGO_DATABASE}")
        print(f"Collection      : {RAW_COLLECTION}")
        print("=" * 70)

        # ----------------------------------------------------
        # Open CSV using streaming
        # ----------------------------------------------------

        with input_path.open(
            mode="r",
            encoding="utf-8-sig",
            newline=""
        ) as csv_file:

            reader = csv.DictReader(csv_file)

            if reader.fieldnames is None:
                raise ValueError(
                    "CSV file does not contain a header."
                )

            print(
                f"CSV columns     : {len(reader.fieldnames)}"
            )

            print(
                f"Columns         : {reader.fieldnames}"
            )

            print("-" * 70)

            batch = []

            # ------------------------------------------------
            # Read one record at a time
            # ------------------------------------------------

            for row_number, row in enumerate(
                reader,
                start=2
            ):

                # --------------------------------------------
                # IMPORTANT:
                # Store original values without cleaning.
                # --------------------------------------------

                raw_document = {
                    "id_run": id_run,
                    "file_source": file_source,
                    "number_row_source": row_number,

                    # MongoDB BSON Date
                    "at_ingested": get_current_utc_time(),

                    "engine_used": "python_batch",

                    # Original CSV record
                    "record_raw": dict(row)
                }

                batch.append(raw_document)

                # --------------------------------------------
                # Send batch to MongoDB
                # --------------------------------------------

                if len(batch) >= batch_size:

                    batch_number += 1

                    batch_start = time.perf_counter()

                    result = collection.insert_many(
                        batch,
                        ordered=False
                    )

                    batch_elapsed = (
                        time.perf_counter()
                        - batch_start
                    )

                    batch_count = len(result.inserted_ids)

                    batch_rate = (
                        batch_count / batch_elapsed
                        if batch_elapsed > 0
                        else 0
                    )

                    total_rows += batch_count

                    print(
                        f"Batch #{batch_number:<6} | "
                        f"Records: {batch_count:<6} | "
                        f"Time: {batch_elapsed:.3f}s | "
                        f"Rate: {batch_rate:,.2f} records/sec"
                    )

                    batch.clear()

            # ------------------------------------------------
            # Insert remaining records
            # ------------------------------------------------

            if batch:

                batch_number += 1

                batch_start = time.perf_counter()

                result = collection.insert_many(
                    batch,
                    ordered=False
                )

                batch_elapsed = (
                    time.perf_counter()
                    - batch_start
                )

                batch_count = len(result.inserted_ids)

                batch_rate = (
                    batch_count / batch_elapsed
                    if batch_elapsed > 0
                    else 0
                )

                total_rows += batch_count

                print(
                    f"Batch #{batch_number:<6} | "
                    f"Records: {batch_count:<6} | "
                    f"Time: {batch_elapsed:.3f}s | "
                    f"Rate: {batch_rate:,.2f} records/sec"
                )

                batch.clear()

        # ----------------------------------------------------
        # Final Metrics
        # ----------------------------------------------------

        elapsed = time.perf_counter() - start_time

        throughput = (
            total_rows / elapsed
            if elapsed > 0
            else 0
        )

        print("=" * 70)
        print("RAW LOAD COMPLETED")
        print("=" * 70)

        print(f"Run ID          : {id_run}")
        print(f"Total batches   : {batch_number}")
        print(f"Total records   : {total_rows:,}")
        print(f"Elapsed time    : {elapsed:.3f} seconds")

        print(
            f"Throughput      : "
            f"{throughput:,.2f} records/sec"
        )

        print("=" * 70)

        return {
            "id_run": id_run,
            "file_source": file_source,
            "engine_used": "python_batch",
            "batch_size": batch_size,
            "batches": batch_number,
            "loaded_raw": total_rows,
            "seconds_elapsed": round(elapsed, 3),
            "throughput": round(throughput, 2),
        }

    finally:

        client.close()


# ============================================================
# Command Line Interface
# ============================================================

def main():

    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Load CSV records into MongoDB orders_raw "
            "using Python Batch Streaming."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to input CSV file."
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=(
            f"Number of records per batch. "
            f"Default: {BATCH_SIZE}"
        )
    )

    args = parser.parse_args()

    try:

        load_csv_to_raw(
            input_file=args.input,
            batch_size=args.batch_size
        )

    except Exception as error:

        print()
        print("ERROR:")
        print(error)

        sys.exit(1)


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    main()