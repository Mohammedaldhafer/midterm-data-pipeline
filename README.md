<div align="center">

# ⚡ Hybrid Big Data Pipeline for E-Commerce Orders

### Enterprise-Style ELT Architecture with Dynamic Engine Routing, Automated Data Quality, Audit Trails, Idempotent Upserts, Incremental Processing & Unified API

<p>
  <img src="https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Apache%20Spark-3.5.9-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark">
  <img src="https://img.shields.io/badge/MongoDB-8.0.4-47A248?style=for-the-badge&logo=mongodb&logoColor=white" alt="MongoDB">
  <img src="https://img.shields.io/badge/FastAPI-0.142.2-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI">
  <img src="https://img.shields.io/badge/PyTest-Automated-0A9B42?style=for-the-badge&logo=pytest&logoColor=white" alt="Tests">
  <img src="https://img.shields.io/badge/Architecture-ELT%20%7C%20Hybrid%20Processing-6F42C1?style=for-the-badge" alt="Architecture">
</p>

<p>
  <b>
    A production-oriented Big Data ELT pipeline for large-scale e-commerce
    order data, combining intelligent file routing, streaming and distributed
    ingestion, automated data quality, audit trails, quarantine handling,
    idempotent writes, incremental Delta processing, analytical reports,
    materialized views, scheduled jobs, and a unified REST API.
  </b>
</p>

</div>

---

## 📑 Table of Contents

- [🎯 Project Overview](#-project-overview)
- [✨ Key Features](#-key-features)
- [🌐 Final Project: API & Advanced Features](#-final-project-api--advanced-features)
- [🏗 Architecture Overview](#️-architecture-overview)
- [🔄 Data Processing Flow](#-data-processing-flow)
- [🧹 Data Quality & Validation](#-data-quality--validation)
- [🔁 Idempotency & Incremental Processing](#-idempotency--incremental-processing)
- [🔎 Queries, Indexes & Explain](#-queries-indexes--explain)
- [📊 Aggregation Reports](#-aggregation-reports)
- [🧮 Materialized Views](#-materialized-views)
- [🕒 Scheduled Jobs](#-scheduled-jobs)
- [🚀 Unified FastAPI](#-unified-fastapi)
- [📊 Engine Benchmarking](#-engine-benchmarking)
- [📂 Project Structure](#-project-structure)
- [🛠 Technology Stack](#️-technology-stack)
- [🚀 Installation & Setup](#-installation--setup)
- [▶️ Execution](#️-execution)
- [🧪 Automated Testing](#-automated-testing)
- [📈 Verification & Results](#-verification--results)
- [📚 Documentation](#-documentation)
- [🎓 Project Context](#-project-context)

---

# 🎯 Project Overview

This project implements a **Hybrid Big Data ELT Pipeline** for processing large-scale e-commerce order data containing realistic data-quality issues.

The system follows an **ELT architecture**:

1. Ingest raw data first.
2. Preserve the original record and ingestion metadata.
3. Apply normalization and quality rules.
4. Classify records as Valid, Corrected, or Quarantined.
5. Upsert recoverable records into MongoDB.
6. Expose queries, aggregations, materialized views, and scheduled operations through a unified API.

A configurable routing layer automatically chooses the processing engine according to input file size:

```text
                       ┌─────────────────────────────┐
                       │      E-Commerce CSV         │
                       │       Dirty / Raw Data      │
                       └──────────────┬──────────────┘
                                      │
                                      ▼
                       ┌─────────────────────────────┐
                       │       File Router           │
                       │       Size Threshold        │
                       └──────────────┬──────────────┘
                                      │
                       ┌──────────────┴──────────────┐
                       │                             │
                  <= 200 MB                      > 200 MB
                       │                             │
                       ▼                             ▼
              ┌─────────────────┐          ┌─────────────────┐
              │ Python Streaming│          │     PySpark     │
              │     Batch       │          │   Distributed   │
              └────────┬────────┘          └────────┬────────┘
                       │                             │
                       └──────────────┬──────────────┘
                                      │
                                      ▼
                       ┌─────────────────────────────┐
                       │          RAW LAYER          │
                       │        orders_raw           │
                       └──────────────┬──────────────┘
                                      │
                                      ▼
                       ┌─────────────────────────────┐
                       │   Data Quality / ELT Engine │
                       └──────────────┬──────────────┘
                                      │
                         ┌────────────┴────────────┐
                         │                         │
                         ▼                         ▼
                  orders_validated         orders_quarantine
                         │
                         ▼
               ┌───────────────────────┐
               │ Queries / Aggregation │
               │ Materialized Views    │
               │ Scheduled Jobs        │
               │ FastAPI               │
               └───────────────────────┘
```

The system is designed to demonstrate practical Big Data engineering concepts including:

- ELT architecture
- Hybrid processing
- Dynamic engine selection
- Distributed processing
- Data quality engineering
- Auditability
- Data quarantine
- Idempotent writes
- Version-based incremental processing
- MongoDB indexing and query optimization
- Aggregation pipelines
- Materialized views
- Scheduled operations
- API-based access
- Automated testing and verification

---

# ✨ Key Features

### 🔀 1. Dynamic Processing Engine Routing

The file router uses a configurable threshold of **200 MB**.

| Dataset Size | Engine |
| --- | --- |
| `<= 200 MB` | Python Streaming Batch |
| `> 200 MB` | PySpark |

The router also reports the input size, configured threshold, selected engine, and decision reason.

---

### 🛡 2. True ELT Architecture

Raw records are first stored in:

```text
orders_raw
```

before destructive filtering or business-rule rejection.

Each raw record keeps ingestion metadata such as:

- `id_run`
- source file
- source row number
- ingestion timestamp
- selected engine
- original record

This supports traceability, reproducibility, debugging, and auditability.

---

### 🧹 3. Automated Data Quality

The pipeline includes multiple quality transformations, including:

- Arabic-Indic digit normalization
- Currency normalization
- Thousand-separator handling
- Date normalization
- Phone normalization
- Email validation
- JSON validation/correction
- Status standardization
- Total reconciliation
- Missing-value handling
- Structural validation

Corrected records retain correction information for audit purposes.

---

### ☣️ 4. Quarantine Layer

Records that cannot be safely recovered are moved to:

```text
orders_quarantine
```

Quarantine records preserve structured diagnostic information such as:

- error codes
- error details
- original record
- run ID
- processing metadata

No unrecoverable record is silently discarded.

---

### 🔁 5. Idempotent Upsert

The business key is:

```text
id_order
```

and MongoDB enforces uniqueness through:

```text
unique_id_order
```

The core pipeline distinguishes:

```text
Insert
Update
Unchanged
```

Repeated execution does not intentionally create duplicate business records.

---

# 🌐 Final Project: API & Advanced Features

The final project extends the midterm implementation with database optimization, analytical reporting, incremental materialized views, scheduled jobs, and a unified FastAPI layer.

These additions are designed to be independent, reproducible, and runnable with evaluator-supplied data rather than hard-coded counts or filenames.

---

## 🔌 Unified FastAPI

The API is implemented in:

```text
src/api.py
```

Swagger UI:

```text
http://127.0.0.1:8000/docs
```

Implemented endpoints:

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/health` | MongoDB/API health check |
| `POST` | `/ingest` | Upload and run the existing hybrid ingestion pipeline |
| `POST` | `/indexes` | Create required query/MV indexes |
| `GET` | `/queries` | List practical queries |
| `GET` | `/queries/{name}` | Execute one named query |
| `GET` | `/aggregations` | List aggregation reports |
| `GET` | `/aggregations/{name}` | Execute one aggregation report |
| `POST` | `/refresh-mv` | Trigger incremental Delta refresh |
| `GET` | `/jobs` | List scheduled jobs |
| `POST` | `/jobs/{name}/run` | Run one job manually |

The `/ingest` endpoint reuses the existing `src.main.run_pipeline()` path instead of introducing a separate ingestion implementation.

### Recommended Evaluator Flow

```text
1. Start MongoDB
2. Initialize MongoDB collections
3. Create/apply indexes
4. Start FastAPI
5. Open /docs
6. Upload evaluator CSV through POST /ingest
7. Run named queries and aggregation reports
8. Refresh materialized views when Delta data is available
9. Inspect scheduled jobs and run them manually when required
```

`POST /ingest` expects a multipart file upload using the field name `file`. The optional `batch_size` query parameter controls the existing pipeline batch size.

Example:

```text
POST /ingest?batch_size=5000
Content-Type: multipart/form-data

file=<input.csv>
```

The endpoint passes the uploaded file through the same file router and pipeline used by the original project.

---

# 🔎 Queries, Indexes & Explain

Five practical MongoDB queries are implemented:

```text
1. recent_orders_by_city
2. city_orders_by_date
3. customer_orders
4. orders_by_status
5. orders_by_status_and_city
```

The query implementation is in:

```text
src/queries.py
```

### Query Indexes

The following indexes are used for the final query workload:

```text
idx_city_order_date
    { city: 1, order_date: 1 }

idx_customer_order_date
    { customer_id: 1, order_date: 1 }

idx_status_city_order_date
    { status: 1, city: 1, order_date: 1 }
```

The project also retains the business-key index:

```text
unique_id_order
    { id_order: 1 }, unique=True
```

### Explain Evidence

Three representative queries were compared using:

```text
explain("executionStats")
```

The recorded evidence is stored in:

```text
reports/query_explain.md
```

| Query | Before: Docs / Keys / Time | After: Docs / Keys / Time |
| --- | --- | --- |
| City query | `83 / 0 / 2 ms` | `10 / 10 / 13 ms` |
| Status query | `48 / 0 / 0 ms` | `10 / 10 / 25 ms` |
| Status + City query | `694 / 0 / 5 ms` | `10 / 10 / 7 ms` |

The important optimization signal is the large reduction in **documents examined** and the appearance of targeted index scans. Small execution-time differences are treated as environment-dependent rather than as proof of a universal latency improvement.

---

# 📊 Aggregation Reports

The MongoDB aggregation layer is implemented in:

```text
src/aggregations.py
```

Seven named reports are available:

```text
1. sales_by_city
2. orders_by_status
3. sales_by_month
4. sales_by_payment_method
5. sales_by_delivery_type
6. top_products
7. top_customers
```

The five core reports used to satisfy the project requirement are:

```text
sales_by_city
orders_by_status
sales_by_month
sales_by_payment_method
sales_by_delivery_type
```

Each report can be run independently from the command line or through the API.

Examples:

```powershell
python -m src.aggregations --report sales_by_city
python -m src.aggregations --report orders_by_status
python -m src.aggregations --report sales_by_month
```

---

# 🧮 Materialized Views

Two materialized views are implemented:

```text
daily_sales_summary
top_products_summary
```

The implementation is in:

```text
src/materialized_views.py
```

### One-Time Initialization

The views can be initialized once from the existing validated dataset:

```powershell
python -m src.materialized_views --init
```

A verification run on the current project dataset produced:

```text
daily_sales_summary  : 121
top_products_summary : 6
```

These counts are dataset-dependent verification evidence and are **not** required values for evaluator-supplied data.

### Preview

```powershell
python -m src.materialized_views --show
```

### Incremental Refresh

Ongoing updates do **not** require rebuilding both views.

The incremental path is:

```text
Delta CSV
    │
    ▼
incremental_loader.py
    │
    ├── New order      → Insert
    ├── Newer version  → Update
    ├── Same version   → Unchanged
    └── Older version  → Ignored
    │
    ▼
apply_incremental_change()
    │
    ├── Remove old contribution
    └── Add new contribution
    │
    ▼
Materialized Views
```

A controlled integration test proved:

```text
Insert        → MV applied = 1
Higher update → MV applied = 1
Replay        → unchanged, MV not reapplied
Older version → ignored, MV not reapplied
```

This is the operational refresh mechanism used after the one-time initialization.

---

# 🕒 Scheduled Jobs

Scheduled jobs are implemented in:

```text
src/jobs.py
```

Two real jobs are registered:

| Job | Fixed Schedule | Purpose |
| --- | --- | --- |
| `refresh_delta` | Daily at 02:00 | Process pending Delta CSV files incrementally |
| `aggregation_snapshot` | Daily at 03:00 | Run business aggregation reports and persist a snapshot |

The scheduler uses a lightweight Python standard-library loop, so no external scheduler package is required.

### List Jobs

```powershell
python -m src.jobs --list
```

### Manual Run

```powershell
python -m src.jobs --run refresh_delta
```

```powershell
python -m src.jobs --run aggregation_snapshot
```

### Scheduler Mode

```powershell
python -m src.jobs --schedule
```

### Job Observability

Each execution logs:

- job name
- start time
- end time
- success/failure
- result or error

Runtime logs are written to:

```text
logs/jobs.log
```

MongoDB job history is stored in:

```text
job_runs
```

Aggregation snapshots are stored in:

```text
aggregation_job_snapshots
```

A manual `aggregation_snapshot` execution was successfully completed during final-project verification.

---

# 🏗 Architecture Overview

```text
                  ┌──────────────────────────────┐
                  │      Dirty CSV Source       │
                  └──────────────┬───────────────┘
                                 │
                                 ▼
                  ┌──────────────────────────────┐
                  │        File Router           │
                  │      Threshold = 200 MB      │
                  └──────────────┬───────────────┘
                                 │
                    ┌────────────┴────────────┐
                    │                         │
                    ▼                         ▼
             ┌──────────────┐         ┌──────────────┐
             │ Python Batch │         │   PySpark    │
             │  Streaming   │         │ Distributed  │
             └──────┬───────┘         └──────┬───────┘
                    │                        │
                    └───────────┬────────────┘
                                │
                                ▼
                     ┌────────────────────┐
                     │     orders_raw     │
                     │   RAW / Audit Data │
                     └─────────┬──────────┘
                               │
                               ▼
                     ┌────────────────────┐
                     │ Quality / ELT      │
                     │ Normalization      │
                     │ Classification     │
                     │ Audit Trail        │
                     └─────────┬──────────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
                    ▼                     ▼
             ┌───────────────┐    ┌────────────────┐
             │orders_validated│    │orders_quarantine│
             └───────┬───────┘    └────────────────┘
                     │
          ┌──────────┼───────────┬──────────────┐
          │          │           │              │
          ▼          ▼           ▼              ▼
      Queries   Aggregations     MVs        Incremental
          │          │           │            Delta
          └──────────┴─────┬─────┴──────────────┘
                           │
                           ▼
                    Scheduled Jobs
                           │
                           ▼
                     FastAPI /docs
```

---

# 🔄 Data Processing Flow

```text
1. Input CSV
      │
      ▼
2. File Size Detection
      │
      ▼
3. Engine Selection
      ├── Python Batch
      └── PySpark
      │
      ▼
4. Raw Ingestion
      │
      ▼
5. Data Normalization
      │
      ▼
6. Quality Rule Execution
      │
      ▼
7. Classification
      ├── Valid
      ├── Corrected
      └── Quarantined
      │
      ▼
8. MongoDB Upsert
      │
      ▼
9. Consistency Verification
      │
      ▼
10. Reports / Materialized Views / Jobs / API
```

---

# 🧹 Data Quality & Validation

The main quality-processing categories are:

| Category | Example Issue | Action |
| --- | --- | --- |
| Numeric | Arabic-Indic digits | Normalize |
| Currency | Mixed formats | Normalize |
| Price | Thousand separators | Clean and parse |
| Dates | Multiple representations | Standardize |
| Phone | Non-uniform formatting | Normalize |
| Email | Invalid formatting | Validate |
| JSON | Corrupted items JSON | Repair / validate |
| Status | Equivalent status labels | Standardize |
| Totals | Inconsistent totals | Reconcile |
| Structure | Missing required data | Quarantine |

Records are classified as:

```text
VALID
  └── No correction required

CORRECTED
  └── Recoverable issues fixed automatically

QUARANTINED
  └── Unrecoverable structural or business-rule issue
```

---

# 🔁 Idempotency & Incremental Processing

## Core Pipeline

The core pipeline uses:

```text
id_order
```

as the business key and a unique MongoDB index to prevent duplicate business records.

## Path B: Incremental Delta

The independent Delta flow uses:

```text
order_id + version
```

with the following semantics:

| Condition | Action |
| --- | --- |
| New order | Insert |
| Higher version | Update |
| Same version | Unchanged |
| Older version | Ignored |

The same Delta can be replayed safely.

The controlled evidence verified:

```text
Initial Insert
    ↓
Higher Version Update
    ↓
Same Version Replay
    ↓
Older Version
```

with the expected outcomes and no repeated MV contribution.

---

# 📊 Engine Benchmarking

The large project run processed:

| Metric | Result |
| --- | ---: |
| Raw records | 30,000,000 |
| Valid | 12,160,586 |
| Corrected | 15,520,535 |
| Quarantine | 2,318,879 |
| Inserted | 27,490,792 |
| Updated | 190,329 |
| Unchanged | 0 |
| Processing time | 20,969.624 sec |
| Throughput | 1,430.64 records/sec |

The consistency equation was satisfied:

```text
12,160,586
+ 15,520,535
+ 2,318,879
----------------
= 30,000,000
```

Therefore:

```text
RAW = VALID + CORRECTED + QUARANTINE
```

---

# 📂 Project Structure

```text
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
│   ├── results.md
│   └── query_explain.md
│
├── src/
│   ├── __init__.py
│   ├── aggregations.py
│   ├── api.py
│   ├── batch_loader.py
│   ├── create_small_sample.py
│   ├── elt_pipeline.py
│   ├── file_router.py
│   ├── incremental_loader.py
│   ├── jobs.py
│   ├── main.py
│   ├── materialized_views.py
│   ├── metrics.py
│   ├── mongo_setup.py
│   ├── quality_rules.py
│   ├── queries.py
│   ├── spark_loader.py
│   └── upsert_manager.py
│
├── tests/
│   ├── test_classification.py
│   └── test_cleaning_rules.py
│
├── .gitignore
├── example.env
├── requirements.txt
└── README.md
```

Runtime artifacts such as `.venv`, large CSV datasets, Python caches, and `logs/` are intentionally excluded from Git.

---

# 🛠 Technology Stack

| Technology | Purpose |
| --- | --- |
| Python 3.11 | Core implementation |
| PySpark 3.5.9 | Large-scale distributed processing |
| MongoDB 8.0.4 | Data storage and analytics |
| PyMongo | MongoDB integration |
| MongoDB Spark Connector | Spark-to-MongoDB writes |
| FastAPI | Unified REST API |
| Uvicorn | ASGI server |
| python-multipart | Multipart upload support |
| pytest | Automated tests |
| JSON / CSV | Data interchange and reports |

---

# 🚀 Installation & Setup

## Prerequisites

Install:

```text
Python 3.11
MongoDB
Java / Spark environment
```

For the PySpark route on Windows, configure the Spark/Hadoop environment required by the local installation.

---

## 1. Clone the Repository

```powershell
git clone https://github.com/Mohammedaldhafer/midterm-data-pipeline.git
cd midterm-data-pipeline
```

---

## 2. Create the Virtual Environment

```powershell
python -m venv .venv
```

Activate on Windows:

```powershell
.\.venv\Scripts\Activate.ps1
```

---

## 3. Install Dependencies

```powershell
python -m pip install -r .\requirements.txt
```

---

## 4. Configure Environment Variables

Copy:

```text
example.env
```

to:

```text
.env
```

Windows:

```powershell
Copy-Item .\example.env .\.env
```

The provided defaults target local MongoDB:

```text
mongodb://localhost:27017
```

---

## 5. Start MongoDB

Ensure MongoDB is available at:

```text
mongodb://localhost:27017
```

---

## 6. Initialize MongoDB Collections

```powershell
python .\src\mongo_setup.py
```

---

# ▶️ Execution

## 🌐 Run the Unified API

Recommended for evaluation:

```powershell
python -m uvicorn src.api:app --reload
```

Open:

```text
http://127.0.0.1:8000/docs
```

Swagger provides interactive access to the required endpoints.

---

## 🖥 Run the Main Pipeline

Run the main pipeline with any evaluator-provided CSV:

```powershell
python .\src\main.py --input .\data\<input.csv>
```

For a known local dataset, for example:

```powershell
python .\src\main.py --input .\data\orders_test.csv
```

The router automatically selects Python Batch or PySpark based on the file size. No specific filename or record count is required by the implementation.

---

## 🧪 Create a Smaller Sample

Use any available large CSV as input:

```powershell
python .\src\create_small_sample.py `
  --input .\data\<large-input.csv> `
  --output .\data\orders_sample.csv `
  --rows 100000
```

The input filename is an example only; the pipeline is not tied to a specific dataset name.

---

## 🔁 Run Path B Delta

```powershell
python .\src\incremental_loader.py --input .\data\delta_test.csv
```

Replay the same Delta:

```powershell
python .\src\incremental_loader.py --input .\data\delta_test.csv
```

---

## 🧮 Initialize Materialized Views

Run once when the validated dataset is available:

```powershell
python -m src.materialized_views --init
```

Preview:

```powershell
python -m src.materialized_views --show
```

---

## 🕒 Run Scheduled Jobs

List:

```powershell
python -m src.jobs --list
```

Manual Delta refresh:

```powershell
python -m src.jobs --run refresh_delta
```

Manual aggregation snapshot:

```powershell
python -m src.jobs --run aggregation_snapshot
```

Start the fixed scheduler:

```powershell
python -m src.jobs --schedule
```

---

## 🔎 Run Practical Queries

```powershell
python -m src.queries --demo
```

---

## 📊 Run Aggregation Reports

All reports:

```powershell
python -m src.aggregations
```

One report:

```powershell
python -m src.aggregations --report sales_by_city
```

---

# 🧪 Automated Testing

Run the complete test suite:

```powershell
python -m pytest -q
```

The repository includes automated tests for:

- data cleaning rules
- classification
- correction behavior
- quarantine behavior
- audit-trail generation
- business-rule validation

The final-project integration checks also verified:

```text
Materialized View Insert
Materialized View Update
Idempotent Replay
Older-Version Handling
FastAPI endpoint behavior
Scheduled job execution
```

---

# 📈 Verification & Results

## Large Pipeline Consistency

```text
RAW
30,000,000

VALID
12,160,586

CORRECTED
15,520,535

QUARANTINE
2,318,879
```

Consistency:

```text
12,160,586
+15,520,535
+ 2,318,879
------------
30,000,000
```

Status:

```text
PASS
```

---

## Materialized View Verification

Latest one-time initialization:

```text
daily_sales_summary  : 121
top_products_summary : 6
```

Incremental integration evidence:

```text
INSERT  → applied
UPDATE  → applied
REPLAY  → unchanged
OLDER   → ignored
```

---

## FastAPI Verification

Verified endpoint checks include:

```text
GET  /health                    → 200
GET  /queries                   → 200
GET  /queries/{name}            → 200
GET  /aggregations              → 200
GET  /aggregations/{name}      → 200
GET  /jobs                      → 200
POST /indexes                   → 200
POST /refresh-mv                → 200
POST /jobs/refresh_delta/run    → 200
POST /ingest                    → 200 (real temporary CSV)
```

Swagger:

```text
/docs
```

MongoDB health verification returned:

```text
status = ok
mongodb = true
daily_sales_summary = true
top_products_summary = true
```

---

## Scheduled Job Verification

`refresh_delta` was manually executed successfully with:

```text
files_found = 0
mode = incremental_delta
status = success
```

`aggregation_snapshot` was manually executed successfully and produced a persisted snapshot document.

---

# 📚 Documentation

Technical architecture documentation:

[Technical Architecture](docs/architecture.md)

```text
docs/architecture.md
```

Query Explain evidence:

```text
reports/query_explain.md
```

Pipeline result files:

```text
reports/results.json
reports/results.md
```

Environment template:

```text
example.env
```

---

# 🎓 Project Context

This project is the continuation of a hybrid Big Data pipeline assignment and is designed to demonstrate a complete data-engineering workflow rather than a single processing script.

The implementation combines:

```text
Ingestion
   ↓
ELT
   ↓
Quality & Audit
   ↓
MongoDB
   ↓
Queries & Indexes
   ↓
Aggregation Reports
   ↓
Materialized Views
   ↓
Scheduled Jobs
   ↓
Unified FastAPI
```

The design intentionally avoids hard-coded evaluation results and supports different input datasets through dynamic parameter discovery and reusable processing functions.

The recorded 30-million-record figures in this README are historical verification evidence only; they are not assumptions used by the runtime code and do not need to be reproduced during ordinary final-project evaluation.

---

# 💡 Engineering Highlights

```text
                         ┌──────────────────────┐
                         │      Raw Source      │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │    Dynamic Router    │
                         └──────────┬───────────┘
                                    │
                           ┌────────┴────────┐
                           ▼                 ▼
                    Python Batch         PySpark
                           │                 │
                           └────────┬────────┘
                                    ▼
                            ┌──────────────┐
                            │  RAW Layer   │
                            └──────┬───────┘
                                   ▼
                           ┌───────────────┐
                           │ Quality ELT   │
                           └──────┬────────┘
                                  │
                         ┌────────┴────────┐
                         ▼                 ▼
                    Validated         Quarantine
                         │
                         ▼
                      MongoDB
                         │
           ┌─────────────┼──────────────┐
           ▼             ▼              ▼
        Queries     Aggregations      MVs
           │             │              │
           └─────────────┴──────┬───────┘
                                ▼
                           Scheduled Jobs
                                │
                                ▼
                           FastAPI /docs
```

The result is a unified, auditable, and extensible Big Data architecture suitable for demonstrating practical ELT, MongoDB analytics, incremental processing, and API-based operations.

---

# 👨‍💻 Author

**Mohammed Aldhafer**

Artificial Intelligence Student | Python | ML | Data Engineering

---

## 🔗 Repository

```text
https://github.com/Mohammedaldhafer/midterm-data-pipeline
```
