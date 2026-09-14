# Midterm Data Pipeline

A hybrid Big Data ETL pipeline using Python Batch, PySpark, and MongoDB.

## 1. Project Overview

This project implements a hybrid data pipeline for processing large CSV order datasets.

The pipeline supports:

- Automatic engine selection based on file size.
- Python Batch processing for smaller files.
- PySpark processing for large files.
- MongoDB as the target database.
- Raw data ingestion before transformation.
- Data quality validation, correction, and quarantine.
- Upsert and idempotency.
- Incremental Delta loading with version-based conflict handling.
- Processing metrics and JSON results.

## 2. Architecture

The main processing flow is:

CSV File
    |
    v
File Router
    |
    +---- <= 200 MB ----> Python Batch
    |
    +---- > 200 MB ----> PySpark
                              |
                              v
                         MongoDB Raw
                              |
                              v
                         ELT / Quality
                              |
                +-------------+-------------+
                |             |             |
                v             v             v
             Valid        Corrected     Quarantine
                |
                v
          Upsert / MongoDB

Path B incremental flow:

Delta CSV
    |
    v
Incremental Loader
    |
    v
Version-based conflict handling
    |
    +---- New order ------> Insert
    |
    +---- Newer version --> Update
    |
    +---- Same version ---> Unchanged
    |
    +---- Older version --> Ignored
3. Technologies
Python 3.11
PySpark 3.5.x
MongoDB
PyMongo
pytest

Dependencies are listed in requirements.txt.

4. Requirements

Install the required Python packages:

pip install -r requirements.txt

Required services:

Python 3.11
MongoDB running locally on port 27017
Java/Spark environment for PySpark execution
5. MongoDB

Database:

midterm_data_pipeline

Main collections:

orders_raw
orders_validated
orders_quarantine

The MongoDB setup script creates validators and indexes.

Run:

python .\src\mongo_setup.py
6. File Router

The router uses a configurable threshold of:

200 MB

Decision:

File size <= 200 MB → Python Batch
File size > 200 MB → PySpark

The router reports:

File size
Threshold
Selected engine
Reason for the decision
7. Running the Main Pipeline

Run the main pipeline with:

python .\src\main.py --input .\data\orders_test.csv

For a different batch size:

python .\src\main.py --input .\data\orders_test.csv --batch-size 5000

The pipeline automatically selects Python Batch or PySpark according to the file size.

8. Small Sample

The project includes a streaming utility for creating a smaller sample from a large CSV:

python .\src\create_small_sample.py `
  --input .\data\orders_huge_mixed_quality.csv `
  --output .\data\orders_sample.csv `
  --rows 100000

The generated CSV files are intentionally excluded from Git because the project may contain very large datasets.

9. ELT and Data Quality

The pipeline loads records into the Raw collection before applying data quality processing.

Records are classified into:

Valid
Corrected
Quarantined

Quarantine records include error information to support auditability.

The pipeline also checks the count consistency:

raw = valid + corrected + quarantine
10. Upsert and Idempotency

Orders use id_order as the business key.

The Upsert logic distinguishes:

Inserted records
Updated records
Unchanged records

A unique MongoDB index prevents duplicate id_order values.

Reprocessing the same data does not create duplicate business records.

11. Path B: Incremental Loading

Path B is implemented using an independent Delta CSV.

The Delta contains a version field.

Version rules:

New order                 -> Insert
Newer version             -> Update
Same version              -> Unchanged
Older version             -> Ignored

The incremental loader reports:

count_inserted
count_updated
count_unchanged
count_older
Incremental example

Run:

python .\src\incremental_loader.py --input .\data\delta_test.csv

Replay the same Delta:

python .\src\incremental_loader.py --input .\data\delta_test.csv

A replay of the same version is classified as unchanged rather than creating a duplicate.

A Delta containing a higher version updates the existing order.

A Delta containing an older version is ignored.

12. Path B Experimental Evidence

The following tests were performed on a controlled Delta record.

Delta Insert

Result:

count_processed : 1
count_inserted  : 1
count_updated   : 0
count_unchanged : 0
count_older     : 0
Replay of the Same Delta

Result:

count_processed : 1
count_inserted  : 0
count_updated   : 0
count_unchanged : 1
count_older     : 0

This demonstrates idempotent replay.

Higher Version Update

A Delta with version=2 for an existing version=1 order produced:

count_processed : 1
count_inserted  : 0
count_updated   : 1
count_unchanged : 0
count_older     : 0
Older Version Conflict

A Delta with version=1 after the record had reached version=2 produced:

count_processed : 1
count_inserted  : 0
count_updated   : 0
count_unchanged : 0
count_older     : 1

MongoDB verification confirmed that the order remained at:

version = 2
status = تم التحديث
total_amount = 1500
13. Large Pipeline Result

A large pipeline execution processed:

Raw        : 30,000,000
Valid      : 12,160,586
Corrected  : 15,520,535
Quarantine : 2,318,879

The count consistency equation is satisfied:

12,160,586 + 15,520,535 + 2,318,879
= 30,000,000

Upsert results:

Inserted  : 27,490,792
Updated   : 190,329
Unchanged : 0

Processing time:

20,969.624 seconds

Throughput:

1,430.64 records/second
14. Test Status

The project includes automated router tests.

Current test result:

2 passed

Run:

pytest -q
15. Project Structure
midterm-data-pipeline/
│
├── config/
│   └── settings.py
│
├── data/
│   └── .gitkeep
│
├── docs/
│   └── architecture.md
│
├── reports/
│   ├── results.json
│   └── results.md
│
├── src/
│   ├── __init__.py
│   ├── batch_loader.py
│   ├── create_small_sample.py
│   ├── elt_pipeline.py
│   ├── file_router.py
│   ├── incremental_loader.py
│   ├── main.py
│   ├── metrics.py
│   ├── mongo_setup.py
│   ├── quality_rules.py
│   ├── spark_loader.py
│   └── upsert_manager.py
│
├── tests/
│   ├── test_classification.py
│   └── test_cleaning_rules.py
│
├── .gitignore
├── requirements.txt
└── README.md
16. Generated and Local Files

Large CSV datasets, runtime logs, Python cache files, and the virtual environment are excluded from Git using .gitignore.

This keeps the repository suitable for GitHub while preserving the source code and documentation.

17. Reproducibility
Start MongoDB.
Create/validate MongoDB collections:
python .\src\mongo_setup.py
Install dependencies:
pip install -r requirements.txt
Run the main pipeline:
python .\src\main.py --input .\data\orders_test.csv
Run tests:
pytest -q
Run the Path B Delta demonstration:
python .\src\incremental_loader.py --input .\data\delta_test.csv
18. Notes

The repository intentionally does not include the large CSV datasets or runtime log files in Git.

The Path B demonstration uses a small controlled Delta dataset so that Insert, Update, Replay, and Older-Version conflict behavior can be reproduced quickly without rerunning the full 30-million-row pipeline.