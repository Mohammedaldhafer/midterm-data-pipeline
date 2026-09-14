# Pipeline Architecture

## 1. Overview

The project implements a hybrid ETL pipeline using Python Batch, PySpark, and MongoDB.

The engine is selected automatically according to the input file size.

## 2. Main Flow

```text
CSV
 |
 v
File Router
 |
 +-- <= 200 MB --> Python Batch
 |
 +-- > 200 MB --> PySpark
 |
 v
MongoDB orders_raw
 |
 v
ELT / Data Quality
 |
 +--> Validated records
 |
 +--> Corrected records
 |
 +--> Quarantined records
 |
 v
Upsert
 |
 v
MongoDB orders_validated
3. File Router

The router uses a configurable threshold of 200 MB.

Files <= 200 MB use Python Batch.
Files > 200 MB use PySpark.

The router reports the file size, selected engine, threshold, and reason.

4. Raw Layer

All source records are loaded into orders_raw before quality processing.

Raw records contain processing metadata including:

id_run
file_source
number_row_source
at_ingested
engine_used
record_raw
5. Data Quality Layer

The ELT stage evaluates source records and classifies them as:

Valid
Corrected
Quarantined

Quarantined records retain error codes and error details.

The pipeline verifies:

raw = valid + corrected + quarantine
6. MongoDB Collections

The project uses:

orders_raw
orders_validated
orders_quarantine

orders_validated uses a unique index on id_order to prevent duplicate business records.

7. Upsert and Idempotency

The business key is id_order.

The Upsert layer determines whether a record is:

Inserted
Updated
Unchanged

Reprocessing an unchanged record does not create a duplicate.

8. Path B - Incremental Loading

Path B uses an independent Delta CSV.

The Delta contains a version field.

The incremental loader applies the following rules:

New order
    -> Insert

Higher version
    -> Update

Same version
    -> Unchanged

Older version
    -> Ignored

The loader reports:

count_inserted
count_updated
count_unchanged
count_older

This provides a reproducible demonstration of incremental loading, replay, and version conflict handling.

9. Path B Demonstration

The controlled Delta experiment demonstrated:

A new order was inserted.
Replaying the same Delta produced unchanged rather than a duplicate.
A higher version updated the existing order.
An older version was ignored.
MongoDB retained the latest version after the older-version attempt.
10. Performance

The large pipeline execution processed 30,000,000 records.

The recorded result was:

Raw        : 30,000,000
Valid      : 12,160,586
Corrected  : 15,520,535
Quarantine : 2,318,879

The consistency equation was satisfied:

30,000,000 =
12,160,586 +
15,520,535 +
2,318,879

Recorded throughput:

1,430.64 records/second
11. Reproducibility

The main pipeline can be executed using:

python .\src\main.py --input .\data\orders_test.csv

Tests:

pytest -q

Path B:

python .\src\incremental_loader.py --input .\data\delta_test.csv

MongoDB setup:

python .\src\mongo_setup.py