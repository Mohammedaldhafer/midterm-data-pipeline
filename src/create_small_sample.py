import argparse
import csv
import sys
from pathlib import Path


def create_small_sample(
    input_file: str,
    output_file: str,
    rows: int
) -> None:
    """
    Create a reproducible small CSV sample from a large CSV file.

    The file is processed using streaming:
    the entire CSV is never loaded into memory.
    """

    input_path = Path(input_file)
    output_path = Path(output_file)

    # --------------------------------------------------------
    # Validate input
    # --------------------------------------------------------

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file does not exist: {input_path}"
        )

    if rows <= 0:
        raise ValueError(
            "Number of rows must be greater than zero."
        )

    # --------------------------------------------------------
    # Create output directory
    # --------------------------------------------------------

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    # --------------------------------------------------------
    # Read source CSV and write sample
    # --------------------------------------------------------

    rows_written = 0

    with (
        input_path.open(
            mode="r",
            encoding="utf-8-sig",
            newline=""
        ) as source_file,
        output_path.open(
            mode="w",
            encoding="utf-8",
            newline=""
        ) as sample_file
    ):

        reader = csv.reader(source_file)
        writer = csv.writer(sample_file)

        # ----------------------------------------------------
        # Read header
        # ----------------------------------------------------

        try:
            header = next(reader)
        except StopIteration:
            raise ValueError(
                "The input CSV file is empty."
            )

        writer.writerow(header)

        # ----------------------------------------------------
        # Copy only the requested number of data rows
        # ----------------------------------------------------

        for row in reader:

            writer.writerow(row)

            rows_written += 1

            if rows_written >= rows:
                break

    # --------------------------------------------------------
    # Report results
    # --------------------------------------------------------

    print("=" * 60)
    print("Small Sample Creation")
    print("=" * 60)

    print(f"Input file      : {input_path}")
    print(f"Output file     : {output_path}")
    print(f"Requested rows  : {rows:,}")
    print(f"Written rows    : {rows_written:,}")
    print("=" * 60)

    if rows_written < rows:
        print(
            "WARNING: The source file contains fewer rows "
            "than requested."
        )

    print("Sample creation completed successfully.")


def parse_arguments():
    """
    Parse command-line arguments.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Create a reproducible small sample "
            "from a large CSV file."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to the source CSV file."
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Path to the generated sample CSV file."
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=100_000,
        help=(
            "Number of data rows to extract. "
            "Default: 100000"
        )
    )

    return parser.parse_args()


def main():
    """
    Application entry point.
    """

    args = parse_arguments()

    try:
        create_small_sample(
            input_file=args.input,
            output_file=args.output,
            rows=args.rows
        )

    except Exception as error:
        print(
            f"ERROR: {error}",
            file=sys.stderr
        )
        sys.exit(1)


if __name__ == "__main__":
    main()