# Pipeline Architecture

## 1. Purpose

This document describes the current architecture of the final project, including the original hybrid ELT pipeline and the final-phase additions:

- MongoDB queries and indexes
- `explain("executionStats")` evidence
- Aggregation reports
- Incrementally maintained materialized views
- Scheduled jobs
- Unified FastAPI
- Automated verification

The implementation is designed to work with evaluator-supplied datasets and avoids hard-coded filenames, record counts, and query results.

---

## 2. High-Level Architecture

```text
                         ┌─────────────────────────────┐
                         │      CSV / Delta Source     │
                         └──────────────┬──────────────┘
                                        │
                         ┌──────────────▼──────────────┐
                         │        File Router          │
                         │        Threshold: 200 MB    │
                         └──────────────┬──────────────┘
                                        │
                          ┌─────────────┴─────────────┐
                          │                           │
                    <= 200 MB                     > 200 MB
                          │                           │
                          ▼                           ▼
                 ┌────────────────┐        ┌────────────────┐
                 │ Python Batch    │        │ PySpark        │
                 │ Streaming      │        │ Distributed    │
                 └───────┬────────┘        └───────┬────────┘
                         │                         │
                         └────────────┬────────────┘
                                      ▼
                           ┌────────────────────┐
                           │ MongoDB orders_raw │
                           └──────────┬─────────┘
                                      ▼
                           ┌────────────────────┐
                           │ ELT / Data Quality │
                           └──────────┬─────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    │                 │                 │
                    ▼                 ▼                 ▼
                  Valid            Corrected         Quarantine
                    │                 │                 │
                    └────────────┬────┴─────────────────┘
                                 ▼
                         Idempotent Upsert
                                 │
                                 ▼
                    ┌──────────────────────────┐
                    │ MongoDB orders_validated │
                    └────────────┬─────────────┘
                                 │
                  ┌──────────────┼────────────────────┐
                  │              │                    │
                  ▼              ▼                    ▼
              Queries       Aggregations       Materialized Views
                  │              │                    │
                  └──────────────┼────────────────────┘
                                 ▼
                           Scheduled Jobs
                                 │
                                 ▼
                            FastAPI API
```

---

## 3. Main Ingestion Path

### 3.1 File Router

The router in `src/file_router.py` selects the processing engine from the input file size.

```text
File size <= 200 MB  -> Python Batch
File size > 200 MB   -> PySpark
```

The router exposes:

- measured file size
- configured threshold
- selected engine
- decision reason

The threshold is configurable rather than embedded as a dataset-specific assumption.

### 3.2 Raw Ingestion

The selected loader writes source records to:

```text
orders_raw
```

The raw layer preserves traceability metadata such as:

```text
id_run
file_source
number_row_source
at_ingested
engine_used
record_raw
```

This preserves the original input before quality processing.

### 3.3 ELT and Data Quality

The ELT stage evaluates the raw records and classifies them into:

```text
Valid
Corrected
Quarantined
```

The pipeline performs normalization and validation rules such as date, phone, email, currency, status, JSON, missing-value handling, and total reconciliation.

Records that cannot be safely recovered are written to:

```text
orders_quarantine
```

with structured diagnostic information.

The pipeline verifies the accounting invariant:

```text
Raw = Valid + Corrected + Quarantine
```

### 3.4 Idempotent Upsert

Recoverable records are written to:

```text
orders_validated
```

The business key is:

```text
id_order
```

and the collection uses a unique business-key index:

```text
unique_id_order
```

The upsert layer distinguishes:

```text
Insert
Update
Unchanged
```

so reprocessing the same business record does not intentionally create duplicates.

---

## 4. Incremental Delta Path

`src/incremental_loader.py` provides version-based incremental loading for Delta data.

A Delta record contains a version value. The loader applies:

```text
New order           -> Insert
Higher version     -> Update
Same version       -> Unchanged
Older version      -> Older / Ignored
```

The loader reports the counts for each outcome.

The same incremental mechanism also integrates with the materialized-view layer when the required MV collections are available. This allows MV state to be adjusted only for accepted inserts/updates instead of rebuilding all source data.

A controlled verification was completed for:

```text
Insert
Update
Replay / Unchanged
Older-Version Handling
```

---

## 5. Query Layer, Indexes, and Explain

The query implementation is in:

```text
src/queries.py
```

Five practical named queries are provided:

```text
recent_orders_by_city
city_orders_by_date
customer_orders
orders_by_status
orders_by_status_and_city
```

The final query workload uses these phase-2 indexes:

```text
idx_city_order_date
    { city: 1, order_date: 1 }

idx_customer_order_date
    { customer_id: 1, order_date: 1 }

idx_status_city_order_date
    { status: 1, city: 1, order_date: 1 }
```

The existing unique business-key index is also retained:

```text
unique_id_order
    { id_order: 1 }, unique=True
```

### Explain Verification

Three representative queries were measured with:

```text
explain("executionStats")
```

before and after the phase-2 indexes.

```text
Query 1
Before: 83 docs / 0 keys / 2 ms
After : 10 docs / 10 keys / 13 ms

Query 2
Before: 48 docs / 0 keys / 0 ms
After : 10 docs / 10 keys / 25 ms

Query 3
Before: 694 docs / 0 keys / 5 ms
After : 10 docs / 10 keys / 7 ms
```

The strongest evidence of indexing impact is the reduction in documents examined and the use of targeted index scans. Small wall-clock differences are treated as environment-dependent.

Detailed evidence is stored in:

```text
reports/query_explain.md
```

---

## 6. Aggregation Layer

The aggregation implementation is in:

```text
src/aggregations.py
```

Seven named reports are available:

```text
sales_by_city
orders_by_status
sales_by_month
sales_by_payment_method
sales_by_delivery_type
top_products
top_customers
```

The first five provide the core final-project reporting requirement.

Each report can be executed independently from the command line or requested through the API.

Examples:

```powershell
python -m src.aggregations --report sales_by_city
python -m src.aggregations --report orders_by_status
python -m src.aggregations --report sales_by_month
```

---

## 7. Materialized Views

The materialized-view implementation is in:

```text
src/materialized_views.py
```

The project maintains two views:

```text
daily_sales_summary
top_products_summary
```

### 7.1 Initial Initialization

A one-time initialization operation can build the views from the current validated data.

This is different from the normal incremental refresh path.

### 7.2 Incremental Refresh

For accepted inserts and updates, the MV layer applies the contribution change:

```text
Remove old contribution
        +
Add new contribution
        =
Updated summary state
```

This avoids rebuilding all historical validated records for every change.

The integration was verified with:

```text
INSERT  -> MV applied
UPDATE  -> MV applied
REPLAY  -> no duplicate MV change
OLDER   -> ignored
```

---

## 8. Scheduled Jobs

Scheduled-job logic is implemented in:

```text
src/jobs.py
```

The scheduler uses a fixed standard-library scheduling loop with these project schedules:

```text
refresh_delta
    Daily at 02:00

aggregation_snapshot
    Daily at 03:00
```

Each job can also be run manually.

List jobs:

```powershell
python -m src.jobs --list
```

Run the incremental refresh job:

```powershell
python -m src.jobs --run refresh_delta
```

Run the aggregation snapshot job:

```powershell
python -m src.jobs --run aggregation_snapshot
```

Start the scheduler:

```powershell
python -m src.jobs --schedule
```

Execution history is recorded in:

```text
job_runs
```

Aggregation snapshots are recorded in:

```text
aggregation_job_snapshots
```

The job log is written to:

```text
logs/jobs.log
```

Each execution records start/end information and success/failure status.

---

## 9. Unified FastAPI Layer

The API implementation is in:

```text
src/api.py
```

Start the API with:

```powershell
python -m uvicorn src.api:app --reload
```

Swagger UI:

```text
http://127.0.0.1:8000/docs
```

Required endpoints:

| Method | Endpoint | Purpose |
| --- | --- | --- |
| GET | `/health` | API and MongoDB health |
| POST | `/ingest` | Upload and run the existing hybrid pipeline |
| POST | `/indexes` | Create query and MV indexes |
| GET | `/queries` | List named queries |
| GET | `/queries/{name}` | Execute a named query |
| GET | `/aggregations` | List named reports |
| GET | `/aggregations/{name}` | Execute a named report |
| POST | `/refresh-mv` | Run the incremental MV refresh job |
| GET | `/jobs` | List scheduled jobs |
| POST | `/jobs/{name}/run` | Run a scheduled job manually |

The `/ingest` endpoint reuses the existing `src.main.run_pipeline()` workflow through the same router/pipeline path rather than implementing a separate ingestion system.

---

## 10. Verification

The final project was verified at several levels.

### Automated Tests

```powershell
python -m pytest -q
```

Current result:

```text
8 passed
```

### API Verification

Verified successfully:

```text
GET  /health
GET  /docs
GET  /queries
GET  /queries/{name}
GET  /aggregations
GET  /aggregations/{name}
GET  /jobs
POST /indexes
POST /refresh-mv
POST /jobs/refresh_delta/run
POST /ingest
```

The real `/ingest` check used a small temporary CSV, confirmed:

```text
HTTP 200
Engine = python_batch
Raw = 100
Consistency = PASS
```

The temporary records were removed after the test.

### Large Pipeline Evidence

The recorded large-scale execution processed:

```text
Raw        : 30,000,000
Valid      : 12,160,586
Corrected  : 15,520,535
Quarantine : 2,318,879
```

with:

```text
Consistency: PASS
Throughput : 1,430.64 records/second
```

These values are verification evidence for the existing dataset, not hard-coded application assumptions.

---

## 11. Repository Documentation Map

```text
README.md
    Main setup, execution, API, query, report, MV, job, and verification guide

docs/architecture.md
    Technical architecture and data-flow description

reports/query_explain.md
    Before/after Explain evidence

reports/results.json
    Structured pipeline result metadata

reports/results.md
    Human-readable pipeline result summary

example.env
    Non-sensitive environment configuration template

requirements.txt
    Python dependencies
```

---

## 12. Evaluator Reproducibility

The evaluator may provide a different dataset.

The implementation therefore relies on:

- runtime file routing
- dynamic query parameter discovery
- reusable aggregation definitions
- configuration through environment variables
- reusable API/job commands
- database state rather than hard-coded evaluation results

The project does not require rerunning the historical 30-million-record workload merely to exercise the final-phase API, query, MV, or job features.
