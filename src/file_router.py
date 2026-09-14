from pathlib import Path
from typing import Dict, Any

# Import project configuration
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import SMALL_FILE_THRESHOLD_MB


def get_file_size_mb(file_path: str) -> float:
    """
    Return the file size in megabytes.
    """

    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Input file does not exist: {path}"
        )

    if not path.is_file():
        raise ValueError(
            f"Input path is not a file: {path}"
        )

    size_bytes = path.stat().st_size
    size_mb = size_bytes / (1024 * 1024)

    return size_mb


def select_engine(file_path: str) -> Dict[str, Any]:
    """
    Select the processing engine based on file size.

    <= threshold  -> Python Batch
    > threshold   -> PySpark
    """

    file_size_mb = get_file_size_mb(file_path)

    if file_size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
        reason = (
            f"File size ({file_size_mb:.2f} MB) is less than "
            f"or equal to the threshold "
            f"({SMALL_FILE_THRESHOLD_MB} MB)."
        )

    else:
        engine = "pyspark"
        reason = (
            f"File size ({file_size_mb:.2f} MB) is greater "
            f"than the threshold "
            f"({SMALL_FILE_THRESHOLD_MB} MB)."
        )

    return {
        "file_path": str(Path(file_path).resolve()),
        "file_size_mb": round(file_size_mb, 2),
        "threshold_mb": SMALL_FILE_THRESHOLD_MB,
        "engine": engine,
        "reason": reason,
    }


def print_router_decision(decision: Dict[str, Any]) -> None:
    """
    Print the router decision in a readable format.
    """

    print()
    print("=" * 65)
    print("FILE ROUTER")
    print("=" * 65)

    print(f"File path       : {decision['file_path']}")
    print(f"File size       : {decision['file_size_mb']:.2f} MB")
    print(f"Threshold       : {decision['threshold_mb']} MB")
    print(f"Selected engine : {decision['engine']}")
    print(f"Reason          : {decision['reason']}")

    print("=" * 65)
    print()


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(
        description="Select Python Batch or PySpark based on file size."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to the input CSV file."
    )

    args = parser.parse_args()

    try:

        decision = select_engine(args.input)

        print_router_decision(decision)

    except Exception as error:

        print(f"ERROR: {error}")
        sys.exit(1)