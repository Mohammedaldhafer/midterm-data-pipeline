import argparse
import json
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import (
    BATCH_SIZE,
    REPORTS_DIR,
)


from file_router import select_engine

from batch_loader import load_csv_to_raw

from spark_loader import load_csv_to_raw as load_csv_spark

from elt_pipeline import process_run


def save_results(results):

    REPORTS_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        REPORTS_DIR /
        "results.json"
    )

    with output_file.open(
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            results,
            file,
            ensure_ascii=False,
            indent=4
        )

    print()
    print(
        f"Results saved to: "
        f"{output_file}"
    )


def print_router_decision(
    decision
):

    print()
    print("=" * 75)
    print("FILE ROUTER")
    print("=" * 75)

    print(
        f"File            : "
        f"{decision['file_path']}"
    )

    print(
        f"Size            : "
        f"{decision['file_size_mb']:.2f} MB"
    )

    print(
        f"Threshold       : "
        f"{decision['threshold_mb']} MB"
    )

    print(
        f"Engine          : "
        f"{decision['engine']}"
    )

    print(
        f"Reason          : "
        f"{decision['reason']}"
    )

    print("=" * 75)


def run_pipeline(
    input_file: str,
    batch_size: int
):

    pipeline_start = (
        time.perf_counter()
    )

    decision = select_engine(
        input_file
    )

    print_router_decision(
        decision
    )

    if decision["engine"] == "python_batch":

        raw_result = load_csv_to_raw(
            input_file=input_file,
            batch_size=batch_size
        )

    elif decision["engine"] == "pyspark":

        raw_result = load_csv_spark(
            input_file=input_file
        )

    else:

        raise RuntimeError(
            "Unknown processing engine: "
            f"{decision['engine']}"
        )

    id_run = raw_result[
        "id_run"
    ]

    elt_result = process_run(
        id_run
    )

    elapsed = (
        time.perf_counter()
        - pipeline_start
    )

    final_results = {}

    final_results.update(
        raw_result
    )

    final_results.update(
        elt_result
    )

    final_results[
        "file_name"
    ] = Path(
        input_file
    ).name

    final_results[
        "file_size_mb"
    ] = decision[
        "file_size_mb"
    ]

    final_results[
        "used_engine"
    ] = decision[
        "engine"
    ]

    final_results[
        "pipeline_seconds_elapsed"
    ] = round(
        elapsed,
        3
    )

    final_results[
        "router_threshold_mb"
    ] = decision[
        "threshold_mb"
    ]

    final_results[
        "router_reason"
    ] = decision[
        "reason"
    ]

    save_results(
        final_results
    )

    print()
    print("=" * 75)
    print("FULL PIPELINE COMPLETED")
    print("=" * 75)

    print(
        f"id_run              : "
        f"{id_run}"
    )

    print(
        f"Engine              : "
        f"{final_results['used_engine']}"
    )

    # process_run() returns "raw", not "loaded_raw"
    print(
        f"Raw                 : "
        f"{final_results['raw']:,}"
    )

    # process_run() returns "valid"
    print(
        f"Valid               : "
        f"{final_results['valid']:,}"
    )

    # process_run() returns "corrected"
    print(
        f"Corrected           : "
        f"{final_results['corrected']:,}"
    )

    # process_run() returns "quarantine"
    print(
        f"Quarantine          : "
        f"{final_results['quarantine']:,}"
    )

    # process_run() returns "inserted"
    print(
        f"Inserted            : "
        f"{final_results['inserted']:,}"
    )

    # process_run() returns "updated"
    print(
        f"Updated             : "
        f"{final_results['updated']:,}"
    )

    # process_run() returns "unchanged"
    print(
        f"Unchanged           : "
        f"{final_results['unchanged']:,}"
    )

    print(
        f"Pipeline time       : "
        f"{elapsed:.3f} sec"
    )

    print("=" * 75)

    return final_results


def main():

    parser = argparse.ArgumentParser(
        description=(
            "Hybrid Big Data Pipeline: "
            "Python Batch + PySpark + MongoDB"
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Input CSV file."
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Batch size. Default: {BATCH_SIZE}"
    )

    args = parser.parse_args()

    input_path = Path(
        args.input
    )

    if not input_path.exists():

        print(
            f"ERROR: Input file does not exist: "
            f"{input_path}"
        )

        sys.exit(1)

    if args.batch_size <= 0:

        print(
            "ERROR: batch-size must be > 0."
        )

        sys.exit(1)

    try:

        run_pipeline(
            input_file=str(
                input_path
            ),
            batch_size=args.batch_size
        )

    except KeyboardInterrupt:

        print(
            "\nPipeline interrupted."
        )

        sys.exit(1)

    except Exception as error:

        print()
        print("=" * 75)
        print("PIPELINE ERROR")
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