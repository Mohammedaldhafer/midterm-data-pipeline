# Query, Index, and Explain Report

## 1. Dataset

The experiments were performed on the MongoDB collection:

`orders_validated`

Current collection size during the experiment:

`27,485,886 documents`

Existing business-key index:

`unique_id_order` on `id_order`

The final-project experiments added three query-oriented indexes.

---

## 2. Five Practical Queries

The project provides the following five practical MongoDB queries:

### Q1 — Recent Orders by City

Filter orders by `city` and return the most recent orders using `order_date`.

### Q2 — Orders by City and Date Range

Filter orders by `city` and an `order_date` range.

### Q3 — Customer Orders by Date Range

Filter orders by `customer_id` and an `order_date` range.

### Q4 — Orders by Status and Date Range

Filter orders by `status` and an `order_date` range.

### Q5 — Orders by Status, City, and Date Range

Filter orders using `status`, `city`, and an `order_date` range.

---

## 3. Indexes

Three indexes were created to support the practical queries:

```text
idx_city_order_date
(city, order_date)

idx_status_order_date
(status, order_date)

idx_status_city_order_date
(status, city, order_date)
Why these indexes were selected
idx_city_order_date
Supports queries that filter by city and retrieve or sort records by order date.
idx_status_order_date
Supports status-based queries combined with a date range.
idx_status_city_order_date
Supports queries that filter by both status and city and then restrict the result by order date.
The indexes are compound indexes and allow MongoDB to narrow the search using multiple query fields.
4. Explain Before Indexes
The following experiments used:
explain("executionStats")
Q1 — City
nReturned           = 10
totalDocsExamined   = 83
totalKeysExamined   = 0
executionTimeMillis = 2

Q2 — Status
nReturned           = 10
totalDocsExamined   = 48
totalKeysExamined   = 0
executionTimeMillis = 0

Q3 — Status + City
nReturned           = 10
totalDocsExamined   = 694
totalKeysExamined   = 0
executionTimeMillis = 5

Before the new indexes, these queries used no query index keys for the tested filters.
5. Explain After Indexes
The same three queries were executed again after creating the indexes.
Q1 — City
nReturned           = 10
totalDocsExamined   = 10
totalKeysExamined   = 10
executionTimeMillis = 13

Q2 — Status
nReturned           = 10
totalDocsExamined   = 10
totalKeysExamined   = 10
executionTimeMillis = 25

Q3 — Status + City
nReturned           = 10
totalDocsExamined   = 10
totalKeysExamined   = 10
executionTimeMillis = 7

6. Before / After Comparison
Query	Before Docs	After Docs	Before Keys	After Keys
Q1 City	83	10	0	10
Q2 Status	48	10	0	10
Q3 Status + City	694	10	0	10


The number of examined documents decreased substantially:
- Q1: 83 → 10
- Q2: 48 → 10
- Q3: 694 → 10
The largest reduction occurred in Q3, where the examined documents decreased from 694 to 10.
7. Interpretation
The execution statistics show that the new indexes are being used to reduce the number of documents examined for the tested queries.
The main measurable effect in this experiment is the reduction in:
totalDocsExamined
and the appearance of:
totalKeysExamined
after the indexes were created.
The measured execution times are very small because each test returns only 10 documents. Therefore, execution time differences at this scale should not be interpreted as a general performance regression or improvement. The document/key examination statistics provide the clearer evidence of index utilization for this experiment.
8. Conclusion
The project satisfies the query/index requirements by providing:
- Five practical MongoDB queries.
- Three query-oriented indexes.
- Multiple compound indexes.
- executionStats measurements before and after index creation.
- A documented explanation of index selection and observed impact.