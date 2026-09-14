import argparse
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    monotonically_increasing_id,
    struct,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
)


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
    MONGO_URI,
    MONGO_DATABASE,
    RAW_COLLECTION,
)


# ============================================================
# MongoDB Spark Connector
# ============================================================

MONGO_CONNECTOR_PACKAGE = (
    "org.mongodb.spark:mongo-spark-connector_2.12:10.7.0"
)


# ============================================================
# CSV Configuration
# ============================================================

CSV_OPTIONS = {
    "header": "true",
    "inferSchema": "false",
    "mode": "PERMISSIVE",

    # --------------------------------------------------------
    # IMPORTANT:
    # The source CSV contains JSON inside quoted fields.
    # Double quotes inside the field are escaped using "".
    # --------------------------------------------------------

    "quote": '"',
    "escape": '"',

    # Allows a quoted CSV field to span multiple lines.
    "multiLine": "true",

    "encoding": "UTF-8",
}


# ============================================================
# Utility Functions
# ============================================================

def generate_run_id():
    """
    Generate a unique pipeline run ID.
    """

    return str(uuid.uuid4())


def get_current_utc_time():
    """
    Return current UTC timestamp as datetime object.

    MongoDB stores this as BSON Date.
    """

    return datetime.now(timezone.utc)


# ============================================================
# Create Spark Session
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder

        # ----------------------------------------------------
        # Application
        # ----------------------------------------------------

        .appName(
            "MidtermDataPipeline-SparkLoader"
        )

        # ----------------------------------------------------
        # MongoDB Spark Connector
        # ----------------------------------------------------

        .config(
            "spark.jars.packages",
            MONGO_CONNECTOR_PACKAGE
        )

        # ----------------------------------------------------
        # MongoDB connection
        # ----------------------------------------------------

        .config(
            "spark.mongodb.write.connection.uri",
            (
                f"{MONGO_URI}/"
                f"{MONGO_DATABASE}."
                f"{RAW_COLLECTION}"
            )
        )

        .config(
            "spark.mongodb.read.connection.uri",
            (
                f"{MONGO_URI}/"
                f"{MONGO_DATABASE}."
                f"{RAW_COLLECTION}"
            )
        )

        # ----------------------------------------------------
        # MongoDB write operation
        # ----------------------------------------------------

        .config(
            "spark.mongodb.write.operationType",
            "insert"
        )

        # ----------------------------------------------------
        # Spark timezone
        # ----------------------------------------------------

        .config(
            "spark.sql.session.timeZone",
            "UTC"
        )

        # ----------------------------------------------------
        # Windows local Spark
        # ----------------------------------------------------

        .config(
            "spark.driver.bindAddress",
            "127.0.0.1"
        )

        .config(
            "spark.driver.host",
            "127.0.0.1"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# Build Raw Schema
# ============================================================

def build_raw_schema(
    spark,
    input_file
):
    """
    Read the CSV header and create a fixed StringType schema.

    All fields remain strings because this is the RAW layer.
    No cleaning or type conversion is performed here.
    """

    header_reader = (
        spark.read

        .options(
            **CSV_OPTIONS
        )

        .csv(
            input_file
        )
    )

    fields = [
        StructField(
            column_name,
            StringType(),
            True
        )
        for column_name in header_reader.columns
    ]

    return StructType(fields)


# ============================================================
# Read CSV
# ============================================================

def read_csv(
    spark,
    input_file,
    schema
):
    """
    Read CSV using the exact options required for the source
    file, especially quote/escape handling for items_json.
    """

    return (
        spark.read

        .options(
            **CSV_OPTIONS
        )

        .schema(
            schema
        )

        .csv(
            input_file
        )
    )


# ============================================================
# Create RAW Document
# ============================================================

def create_raw_document(
    df,
    schema,
    id_run,
    file_source,
    ingestion_time
):
    """
    Convert the CSV DataFrame into the structure required
    by MongoDB orders_raw.

    Final structure:

        id_run
        file_source
        number_row_source
        at_ingested
        engine_used
        record_raw
    """

    csv_columns = schema.fieldNames()

    # --------------------------------------------------------
    # Create unique Spark source row ID
    # --------------------------------------------------------

    df = df.withColumn(
        "_spark_row_id",
        monotonically_increasing_id()
    )

    # --------------------------------------------------------
    # Source row number
    #
    # CSV row 1 = header
    # First data row = row 2
    # --------------------------------------------------------

    df = df.withColumn(
        "number_row_source",
        (
            col("_spark_row_id") + lit(2)
        ).cast(
            LongType()
        )
    )

    # --------------------------------------------------------
    # Create record_raw
    # --------------------------------------------------------

    raw_fields = [
        col(column_name).alias(column_name)
        for column_name in csv_columns
    ]

    df = df.withColumn(
        "record_raw",
        struct(
            *raw_fields
        )
    )

    # --------------------------------------------------------
    # Add metadata
    # --------------------------------------------------------

    df = (
        df

        .withColumn(
            "id_run",
            lit(id_run)
        )

        .withColumn(
            "file_source",
            lit(file_source)
        )

        .withColumn(
            "at_ingested",
            lit(
                ingestion_time
            ).cast(
                TimestampType()
            )
        )

        .withColumn(
            "engine_used",
            lit(
                "pyspark"
            )
        )
    )

    # --------------------------------------------------------
    # Select final MongoDB RAW structure
    # --------------------------------------------------------

    df = df.select(
        "id_run",
        "file_source",
        "number_row_source",
        "at_ingested",
        "engine_used",
        "record_raw"
    )

    return df


# ============================================================
# Load CSV to MongoDB RAW
# ============================================================

def load_csv_to_raw(
    input_file: str
):

    input_path = Path(
        input_file
    )

    # --------------------------------------------------------
    # Validate input
    # --------------------------------------------------------

    if not input_path.exists():

        raise FileNotFoundError(
            f"Input file does not exist: {input_path}"
        )

    if not input_path.is_file():

        raise ValueError(
            f"Input path is not a file: {input_path}"
        )

    # --------------------------------------------------------
    # Run information
    # --------------------------------------------------------

    id_run = generate_run_id()

    file_source = str(
        input_path.resolve()
    )

    ingestion_time = (
        get_current_utc_time()
    )

    spark = None

    try:

        # ====================================================
        # Start Spark
        # ====================================================

        spark = create_spark_session()

        print()
        print("=" * 75)
        print(
            "PYSPARK RAW LOADER"
        )
        print("=" * 75)

        print(
            f"Run ID          : {id_run}"
        )

        print(
            f"Input file      : {file_source}"
        )

        print(
            f"MongoDB         : {MONGO_DATABASE}"
        )

        print(
            f"Collection      : {RAW_COLLECTION}"
        )

        print(
            f"Connector       : "
            f"{MONGO_CONNECTOR_PACKAGE}"
        )

        print("=" * 75)

        # ====================================================
        # Build Schema
        # ====================================================

        schema = build_raw_schema(
            spark,
            input_file
        )

        print()
        print(
            "Fixed schema created:"
        )

        print(
            schema.simpleString()
        )

        print()
        print(
            f"CSV columns     : "
            f"{len(schema.fieldNames())}"
        )

        print(
            f"Columns         : "
            f"{schema.fieldNames()}"
        )

        # ====================================================
        # Read CSV
        # ====================================================

        start_time = (
            time.perf_counter()
        )

        df = read_csv(
            spark,
            input_file,
            schema
        )

        # ====================================================
        # Create MongoDB RAW document
        # ====================================================

        df = create_raw_document(
            df=df,
            schema=schema,
            id_run=id_run,
            file_source=file_source,
            ingestion_time=ingestion_time
        )

        # ====================================================
        # Partitions
        # ====================================================

        input_partitions = (
            df.rdd.getNumPartitions()
        )

        print()
        print(
            f"Input partitions : "
            f"{input_partitions}"
        )

        # ====================================================
        # Count
        # ====================================================

        read_rows = df.count()

        print(
            f"Read rows        : "
            f"{read_rows:,}"
        )

        # ====================================================
        # Final RAW Schema
        # ====================================================

        print()
        print(
            "Final RAW schema:"
        )

        df.printSchema()

        # ====================================================
        # Verify items_json BEFORE MongoDB
        # ====================================================

        print()
        print(
            "Checking items_json..."
        )

        preview = (
            df.select(
                "record_raw.items_json"
            )
            .limit(1)
            .collect()
        )

        if preview:

            items_json_preview = (
                preview[0]["items_json"]
            )

            print(
                "items_json preview:"
            )

            print(
                items_json_preview
            )

            if (
                items_json_preview is None
                or len(items_json_preview.strip()) == 0
            ):

                raise ValueError(
                    "items_json is empty after CSV parsing."
                )

            # ------------------------------------------------
            # Basic validation only.
            #
            # We do NOT clean the JSON here because this is
            # the RAW layer.
            # ------------------------------------------------

            stripped = (
                items_json_preview.strip()
            )

            if not (
                stripped.startswith("[")
                and stripped.endswith("]")
            ):

                raise ValueError(
                    "items_json was not parsed correctly "
                    "from the CSV source. "
                    "Expected a JSON array starting with '[' "
                    "and ending with ']'."
                )

        # ====================================================
        # Preview RAW document
        # ====================================================

        print()
        print(
            "RAW document preview:"
        )

        df.show(
            1,
            truncate=False
        )

        # ====================================================
        # Write to MongoDB
        # ====================================================

        print()
        print(
            "Writing data to MongoDB..."
        )

        (
            df.write

            .format(
                "mongodb"
            )

            .mode(
                "append"
            )

            .option(
                "spark.mongodb.write.connection.uri",
                (
                    f"{MONGO_URI}/"
                    f"{MONGO_DATABASE}."
                    f"{RAW_COLLECTION}"
                )
            )

            .save()
        )

        # ====================================================
        # Metrics
        # ====================================================

        elapsed = (
            time.perf_counter()
            - start_time
        )

        throughput = (
            read_rows / elapsed
            if elapsed > 0
            else 0
        )

        # ====================================================
        # Final Result
        # ====================================================

        print()
        print("=" * 75)
        print(
            "PYSPARK RAW LOAD COMPLETED"
        )
        print("=" * 75)

        print(
            f"Run ID          : {id_run}"
        )

        print(
            f"Read rows       : {read_rows:,}"
        )

        print(
            f"Loaded raw      : {read_rows:,}"
        )

        print(
            f"Partitions      : "
            f"{input_partitions}"
        )

        print(
            f"Elapsed time    : "
            f"{elapsed:.3f} seconds"
        )

        print(
            f"Throughput      : "
            f"{throughput:,.2f} records/sec"
        )

        print("=" * 75)

        return {
            "id_run": id_run,
            "file_source": file_source,
            "engine_used": "pyspark",
            "read_rows": read_rows,
            "loaded_raw": read_rows,
            "partitions": input_partitions,
            "seconds_elapsed": round(
                elapsed,
                3
            ),
            "throughput": round(
                throughput,
                2
            ),
        }

    finally:

        if spark is not None:

            spark.stop()

            print()
            print(
                "SparkSession stopped successfully."
            )


# ============================================================
# Command Line Interface
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Load a large CSV file into MongoDB "
            "using Apache Spark."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help=(
            "Path to input CSV file."
        )
    )

    args = parser.parse_args()

    try:

        load_csv_to_raw(
            args.input
        )

    except Exception as error:

        print()
        print("=" * 75)
        print(
            "ERROR"
        )
        print("=" * 75)

        print(
            error
        )

        print("=" * 75)

        sys.exit(1)


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    main()