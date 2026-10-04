# Hybrid Big Data ELT Pipeline

## Midterm Project - Big Data

مشروع خط بيانات هجين لمعالجة بيانات الطلبات باستخدام:

- Python Batch
- Apache PySpark
- MongoDB
- ELT
- Data Quality
- Audit Trail
- Quarantine
- Idempotency
- Upsert

---

# 1. Project Idea

يستقبل المشروع ملف CSV يحتوي على بيانات طلبات غير نظيفة.

يقوم النظام أولاً بفحص حجم الملف، ثم يختار محرك المعالجة المناسب تلقائياً:

- الملفات الصغيرة: `Python Batch`
- الملفات الكبيرة: `PySpark`

الحد الافتراضي المستخدم في المشروع:

```text
200 MB
```

بعد اختيار المحرك يتم تطبيق ELT:

```text
Dirty CSV
    |
    v
File Router
    |
    +----------------------+
    |                      |
    v                      v
Python Batch            PySpark
    |                      |
    +----------+-----------+
               |
               v
          orders_raw
               |
               v
      Cleaning + Validation
           |           |
           |           +------> orders_quarantine
           |
           v
       Idempotent Upsert
           |
           v
      orders_validated
           |
           v
      reports/results.json
```

المبدأ الأساسي في المشروع:

> جميع السجلات تدخل إلى Raw أولاً قبل تنفيذ التنظيف أو التحقق من الجودة.

---

# 2. Automatic File Router

يستخدم المشروع Router تلقائياً لاختيار المحرك حسب حجم الملف.

القاعدة:

```text
File Size <= 200 MB
        |
        v
Python Batch
```

```text
File Size > 200 MB
        |
        v
PySpark
```

تم اختبار المسارين عملياً.

### Small File

```text
orders_small_sample.csv
Size: 41.77 MB

Selected Engine:
python_batch
```

### Large File

```text
orders_huge_mixed_quality.csv
Size: 12650.32 MB
≈ 12.35 GB

Selected Engine:
pyspark
```

---

# 3. Project Structure

```text
midterm-data-pipeline/
|
|-- README.md
|-- requirements.txt
|-- .gitignore
|
|-- config/
|   `-- settings.py
|
|-- data/
|   `-- orders_small_sample.csv
|
|-- docs/
|   `-- architecture.md
|
|-- reports/
|   |-- results.json
|   |-- results.md
|   `-- screenshots/
|
|-- src/
|   |-- main.py
|   |-- file_router.py
|   |-- create_small_sample.py
|   |-- batch_loader.py
|   |-- spark_loader.py
|   |-- quality_rules.py
|   |-- classification.py
|   |-- elt_pipeline.py
|   |-- spark_elt_pipeline.py
|   |-- mongo_setup.py
|   |-- metrics.py
|   |-- compare_python_spark.py
|   |-- diagnose_mismatches.py
|   `-- spark_write_smoke_test.py
|
`-- tests/
    |-- test_cleaning_rules.py
    |-- test_classification.py
    |-- test_consistency.py
    |-- test_idempotency.py
    `-- test_router.py
```

---

# 4. Requirements

The project requires:

```text
Python
MongoDB
Apache Spark / PySpark
Java JDK 17
MongoDB Spark Connector
```

Install the Python dependencies using:

```powershell
python -m pip install -r requirements.txt
```

---

# 5. MongoDB

MongoDB should be running locally.

Default connection:

```text
mongodb://localhost:27017
```

Database:

```text
midterm_data_pipeline
```

Main Collections:

```text
orders_raw
orders_validated
orders_quarantine
```

### orders_raw

Contains all records exactly as they arrived before cleaning.

Each Raw record contains metadata such as:

```text
run_id
source_file
source_row_number
ingested_at
engine_used
raw_record
```

### orders_validated

Contains:

```text
Valid
Corrected
```

records.

`order_id` is used as the Stable Business Key.

A Unique Index is used on `order_id`.

Writes are performed using Upsert.

### orders_quarantine

Contains records that cannot be corrected safely.

The record contains the Raw data and the reason/error codes.

---

# 6. Create Small Sample

The large source file should not be manually edited using Excel.

A reproducible sample can be created using:

```powershell
python src\create_small_sample.py --input "path\orders_huge_mixed_quality.csv" --rows 100000
```

Example sample:

```text
Rows: 100000
Size: 41.77 MB
```

---

# 7. Router Check Only

The main entry point of the project is:

```text
src/main.py
```

To check the selected engine without writing data:

```powershell
python src\main.py --input data\orders_small_sample.csv
```

Example result:

```text
File size  : 41.77 MB
Threshold  : 200.00 MB
Engine     : python_batch

HYBRID ROUTER: PASS
```

---

# 8. Run Complete Python Batch Pipeline

For the small sample:

```powershell
python src\main.py --input data\orders_small_sample.csv --execute
```

The program performs:

```text
Router
  |
Python Batch
  |
Raw Load
  |
orders_raw
  |
Cleaning
  |
Classification
  |
Valid / Corrected / Quarantine
  |
Upsert
  |
Metrics
```

The CSV is processed using Streaming batches rather than loading the entire file into memory.

Default Batch Size:

```text
5000 records
```

Progress is displayed for every batch, including:

```text
Batch Number
Rows
Time
Throughput
```

---

# 9. PySpark Environment

PySpark is used for files larger than the configured threshold.

Tested versions:

```text
PySpark: 4.2.0
Java: 17
```

On Windows, make sure `JAVA_HOME` points to a full JDK 17 installation.

Example:

```powershell
$env:JAVA_HOME="PATH_TO_JDK_17"
$env:Path="$env:JAVA_HOME\bin;$env:Path"
```

Verify Java:

```powershell
java -version
```

Verify PySpark:

```powershell
python -c "import pyspark; print(pyspark.__version__)"
```

A small Spark test was executed successfully:

```text
ROWS: 10000
PARTITIONS: 12
PYSPARK TEST: PASS
```

Warnings related to `winutils.exe` or the native Hadoop library on Windows do not stop the local Spark test when the Spark job itself completes successfully.

---

# 10. PySpark Large File

Check routing without executing:

```powershell
python src\main.py --input "path\orders_huge_mixed_quality.csv"
```

Tested result:

```text
File size  : 12650.32 MB
Threshold  : 200.00 MB
Engine     : pyspark

HYBRID ROUTER: PASS
```

The Spark implementation uses:

```text
SparkSession
DataFrame API
Fixed Schema
String Raw Fields
MongoDB Spark Connector
Partitions
Parallel Processing
```

The large file is not loaded into a Pandas DataFrame.

---

# 11. ELT Strategy

The project follows ELT rather than ETL.

### Step 1 - Load

All records are first inserted into:

```text
orders_raw
```

No bad records are silently removed.

### Step 2 - Transform

Cleaning and normalization rules are applied.

### Step 3 - Classification

Every Raw record ends in one of:

```text
Valid
Corrected
Quarantined
```

---

# 12. Data Quality Rules

The project implements multiple automatic cleaning rules, including:

```text
Arabic digits normalization
Thousands separator removal
Currency normalization
Known price words
Phone normalization
Email normalization
Date normalization
Whitespace trimming
Status/value aliases
Numeric conversion
Items JSON validation
Required IDs validation
Negative value validation
Order total recalculation
```

Corrections are only made when the transformation is deterministic and safe.

---

# 13. Audit Trail

Every corrected record keeps information about what was changed.

Example:

```json
{
  "quality_status": "Corrected",
  "corrections": [
    {
      "field": "customer_email",
      "original_value": "user@@mail..com",
      "corrected_value": "user@mail.com",
      "rule_code": "EMAIL_REPEATED_SYMBOLS"
    }
  ]
}
```

This provides traceability between the Raw value and the corrected value.

---

# 14. Quarantine

Records that cannot be safely corrected are stored in:

```text
orders_quarantine
```

Examples of error conditions include:

```text
MISSING_ORDER_ID
MISSING_CUSTOMER_ID
INVALID_IMPOSSIBLE_DATE
CORRUPTED_ITEMS_JSON
EMPTY_ITEMS
UNKNOWN_PRICE
AMBIGUOUS_NEGATIVE_VALUE
DUPLICATE_ORDER_ID
EMAIL_INVALID_UNSAFE
```

No invalid record is silently deleted.

---

# 15. Consistency Rule

For every `run_id`, the following rule must be true:

```text
Raw
=
Valid
+
Corrected
+
Quarantine
```

Example small run:

```text
Raw        = 100000
Valid      = 74859
Corrected  = 17108
Quarantine = 8033
```

Check:

```text
74859 + 17108 + 8033
=
100000
```

Result:

```text
Consistency: PASS
```

---

# 16. Large Dataset Result

A documented PySpark run processed approximately:

```text
30,000,000 Raw records
```

Classification result:

```text
Valid       : 22,466,172
Corrected   : 5,108,915
Quarantine  : 2,424,913
```

Consistency:

```text
22,466,172
+
5,108,915
+
2,424,913
=
30,000,000
```

Result:

```text
PASS
```

Evidence is available under:

```text
reports/screenshots/
```

---

# 17. Idempotency and Upsert

`order_id` is used as the Stable Business Key.

`orders_validated` uses:

```text
Unique Index on order_id
+
Upsert
```

The same input was executed more than once.

Before rerun:

```text
orders_validated = 27,575,087
```

After rerun:

```text
orders_validated = 27,575,087
```

The number of business records did not increase.

Result:

```text
IDEMPOTENCY: PASS
NO DUPLICATE BUSINESS RECORDS
```

This proves that rerunning the same input does not create duplicate records.

---

# 18. Metrics

Runtime metrics are generated after pipeline execution.

Metrics include:

```text
run_id
file_name
file_size_mb
engine_used
read_rows
loaded_raw
count_valid
count_corrected
count_quarantine
seconds_elapsed
throughput
batch_size / partitions
counts_case_error
count_inserted
count_updated
count_unchanged
```

The original documented result is stored in:

```text
reports/results.json
```

New test executions are stored separately using the Run ID, for example:

```text
reports/results_<run_id>.json
```

This prevents previous evidence from being overwritten.

---

# 19. Tests

Run the test suite using:

```powershell
python -m pytest tests -v
```

The project includes tests for:

```text
Cleaning Rules
Classification
Consistency
Idempotency
Router
```

Documented test result:

```text
16 passed
```

Evidence:

```text
reports/screenshots/11_pytest_16_passed.png
```

---

# 20. Evidence Screenshots

The project contains evidence for the required practical demonstration.

Examples:

```text
Router - Python Batch
Router - PySpark
Raw Metadata
Corrected Record
Quarantine Record
Unique Index
Schema Validation
Consistency Check
Spark UI Jobs
Spark UI Stages / Tasks
Pytest Results
```

Location:

```text
reports/screenshots/
```

---

# 21. Main Execution Summary

The complete project workflow is:

```text
Input CSV
   |
   v
File Router
   |
   +--------------------+
   |                    |
   v                    v
Python Batch         PySpark
   |                    |
   +---------+----------+
             |
             v
        orders_raw
             |
             v
     Quality Rules
             |
             v
      Classification
        /         \
       v           v
Validated      Quarantine
       |
       v
Idempotent Upsert
       |
       v
orders_validated
       |
       v
Metrics / Results
```

---

# 22. Final Verification

The project verifies:

```text
[PASS] Small file selects Python Batch
[PASS] Large file selects PySpark
[PASS] Raw data is loaded before cleaning
[PASS] Streaming Batch loading
[PASS] PySpark DataFrame processing
[PASS] Fixed Spark Schema
[PASS] Data Quality Rules
[PASS] Audit Trail
[PASS] Quarantine
[PASS] Stable Business Key
[PASS] Unique Index
[PASS] Upsert
[PASS] Idempotency
[PASS] Consistency Rule
[PASS] Metrics
[PASS] Automated Tests
[PASS] Spark UI evidence
[PASS] MongoDB evidence
```

---

# Conclusion

The project implements a Hybrid Big Data ELT Pipeline that automatically selects Python Batch or PySpark according to the input file size.

All source records are preserved in the Raw layer before cleaning. Records are then cleaned, validated and classified into Valid, Corrected or Quarantine.

The final business state is protected using a Stable Business Key, Unique Index, Idempotent Upsert and consistency checks.

The pipeline also records execution metrics and provides documented evidence for Python Batch, PySpark, MongoDB, Data Quality, Idempotency and testing.



---

# Phase 2 - Final Project Additions

Phase 2 extends the existing Hybrid Big Data ELT Pipeline without replacing the Midterm implementation.

The additions include MongoDB Queries, Indexes with Explain evidence, Aggregation Reports, Materialized Views with Incremental Refresh, Scheduled Jobs, and a Unified FastAPI.

## 23. Phase 2 Structure

Phase 2 source files:

```text
src/
└── phase2/
    ├── __init__.py
    ├── queries.py
    ├── indexes.py
    ├── aggregations.py
    ├── materialized_views.py
    ├── jobs.py
    └── api.py
```

Phase 2 generated evidence and reports:

```text
reports/
└── phase2/
    ├── explain_before.json
    ├── explain_after.json
    ├── aggregations/
    │   ├── sales_by_city.json
    │   ├── top_customers.json
    │   ├── sales_by_period.json
    │   ├── orders_by_status.json
    │   └── payment_status_summary.json
    └── jobs/
        ├── job_execution_log.jsonl
        ├── job_state.json
        └── materialized_views_report_latest.json
```

---

## 24. Practical Queries

The project implements five practical MongoDB queries:

| Query | Purpose |
|---|---|
| `orders_by_date_range` | Retrieve orders within a date range |
| `customer_order_history` | Retrieve the order history of a customer |
| `orders_by_city_status` | Filter orders by city and status |
| `high_value_orders` | Retrieve orders above a specified total amount |
| `orders_by_payment_status` | Retrieve orders by payment status |

List all queries:

```powershell
python -m src.phase2.queries --list
```

The queries are parameterized and do not depend on fixed results or fixed row counts.

---

## 25. MongoDB Indexes

Phase 2 provides indexes supporting the practical queries.

| Index | Fields | Type |
|---|---|---|
| `idx_order_date` | `order_date` | Single |
| `idx_customer_date` | `customer_id`, `order_date` | Compound |
| `idx_city_status_date` | `city`, `status`, `order_date` | Compound |
| `idx_total_amount` | `total_amount` | Single |
| `idx_payment_status_date` | `payment_status`, `order_date` | Compound |
| `idx_source_ingested_at_mv` | `source_ingested_at` | Single |

The compound indexes support queries that filter and sort using more than one field.

The `idx_source_ingested_at_mv` index supports Incremental Refresh for Materialized Views.

---

## 26. Explain Before and After Indexes

MongoDB execution statistics were captured for three queries before and after creating the Phase 2 indexes.

Evidence files:

```text
reports/phase2/explain_before.json
reports/phase2/explain_after.json
```

### orders_by_date_range

Before:

```text
COLLSCAN
Documents Examined: 3038
Keys Examined: 0
Returned: 20
```

After:

```text
IXSCAN
Documents Examined: 20
Keys Examined: 20
Returned: 20
```

### customer_order_history

Before:

```text
COLLSCAN
Documents Examined: 27575087
Keys Examined: 0
Returned: 1
Execution Time: 39670 ms
```

After:

```text
IXSCAN
Documents Examined: 1
Keys Examined: 1
Returned: 1
Execution Time: 1 ms
```

### orders_by_city_status

Before:

```text
COLLSCAN
Documents Examined: 1303
Keys Examined: 0
Returned: 20
```

After:

```text
IXSCAN
Documents Examined: 20
Keys Examined: 20
Returned: 20
```

The results demonstrate the transition from Collection Scan (`COLLSCAN`) to Index Scan (`IXSCAN`) and a significant reduction in documents examined.

---

## 27. Aggregation Reports

Five independent MongoDB Aggregation Reports are implemented:

| Aggregation | Description |
|---|---|
| `sales_by_city` | Sales and order statistics by city |
| `top_customers` | Customers ranked by total spending |
| `sales_by_period` | Daily sales statistics |
| `orders_by_status` | Order distribution by status |
| `payment_status_summary` | Order and payment totals by payment status |

List reports:

```powershell
python -m src.phase2.aggregations --list
```

Example execution:

```powershell
python -m src.phase2.aggregations --name orders_by_status --limit 10 --save
```

Generated reports are saved under:

```text
reports/phase2/aggregations/
```

All reports execute against real data in MongoDB.

---

## 28. Materialized Views

Two Materialized Views are implemented:

```text
daily_sales_summary
city_sales_summary
```

### daily_sales_summary

Stores daily summary information including:

```text
period
order_count
total_sales
average_order_value
refreshed_at
source_watermark
```

### city_sales_summary

Stores summary information by city including:

```text
city
order_count
total_sales
average_order_value
refreshed_at
source_watermark
```

List Materialized Views:

```powershell
python -m src.phase2.materialized_views --list
```

Check their current status:

```powershell
python -m src.phase2.materialized_views --status
```

Refresh one Materialized View:

```powershell
python -m src.phase2.materialized_views --refresh daily_sales_summary
```

Refresh all Materialized Views:

```powershell
python -m src.phase2.materialized_views --refresh-all
```

A full rebuild is available only when explicitly needed:

```powershell
python -m src.phase2.materialized_views --refresh-all --rebuild
```

Normal refresh operations should not use `--rebuild`.

---

## 29. Incremental Refresh

The Materialized Views use:

```text
source_ingested_at
```

as the Incremental Refresh watermark.

This field was selected because the existing Phase 1 ingestion process updates it when a record is ingested or reprocessed.

The supporting index is:

```text
idx_source_ingested_at_mv
```

The refresh mechanism supports three modes:

```text
FULL_BUILD
INCREMENTAL
NO_CHANGES
```

### FULL_BUILD

Used during the first creation of a Materialized View or when a rebuild is explicitly requested.

### INCREMENTAL

The system detects source documents whose `source_ingested_at` is newer than the previous watermark.

It then determines the affected buckets and recalculates only those buckets instead of rebuilding all Materialized Views from scratch.

The incremental mechanism was tested successfully.

Observed daily result:

```text
view_name: daily_sales_summary
refresh_mode: INCREMENTAL
watermark_field: source_ingested_at
affected_buckets: 116
document_count: 121
```

Observed city result:

```text
view_name: city_sales_summary
refresh_mode: INCREMENTAL
watermark_field: source_ingested_at
affected_buckets: 10
document_count: 10
```

A subsequent refresh without new source changes returned:

```text
refresh_mode: NO_CHANGES
affected_buckets: 0
```

This confirms that a Full Build is not performed on every refresh.

---

## 30. Scheduled Jobs

Two Scheduled Jobs are implemented:

| Job | Schedule | Purpose |
|---|---|---|
| `refresh_materialized_views` | Every 1 hour | Refresh Materialized Views |
| `export_materialized_views_report` | Every 24 hours | Export a periodic Materialized Views report |

List jobs:

```powershell
python -m src.phase2.jobs --list
```

Run the Materialized Views refresh manually:

```powershell
python -m src.phase2.jobs --run refresh_materialized_views
```

Run the report export manually:

```powershell
python -m src.phase2.jobs --run export_materialized_views_report
```

Run only jobs whose schedules are due:

```powershell
python -m src.phase2.jobs --run-due
```

Start the scheduler loop:

```powershell
python -m src.phase2.jobs --scheduler
```

The scheduler process must remain running for automatic periodic execution.

Stop it using:

```text
Ctrl + C
```

Job logs record:

```text
Job name
Start time
End time
Duration
SUCCESS or FAILED
Execution result
```

Execution logs:

```text
reports/phase2/jobs/job_execution_log.jsonl
```

Latest job state:

```text
reports/phase2/jobs/job_state.json
```

---

## 31. Unified FastAPI

Phase 2 includes a Unified FastAPI wrapper.

The API does not create a second ingestion implementation.

`POST /ingest` directly calls the existing Phase 1 function:

```python
run_pipeline(
    input_path=input_path,
    execute=True,
)
```

Therefore the existing automatic Router, Python Batch path, PySpark path, Raw loading, cleaning, validation, Upsert, and metrics remain the official ingestion path.

---

## 32. Start FastAPI

From the project root:

```powershell
python -m uvicorn src.phase2.api:app --host 127.0.0.1 --port 8000
```

Swagger UI:

```text
http://127.0.0.1:8000/docs
```

API root:

```text
http://127.0.0.1:8000/
```

Stop the server using:

```text
Ctrl + C
```

---

## 33. API Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/health` | Check API and MongoDB connectivity |
| POST | `/ingest` | Run the existing Phase 1 ingestion pipeline |
| POST | `/indexes` | Create or verify Phase 2 indexes |
| GET | `/queries` | List practical queries |
| GET | `/queries/{name}` | Execute a named query |
| GET | `/aggregations` | List Aggregation Reports |
| GET | `/aggregations/{name}` | Execute a named Aggregation Report |
| POST | `/refresh-mv` | Refresh Materialized Views |
| GET | `/jobs` | List jobs and their latest state |
| POST | `/jobs/{name}/run` | Run a job manually |

An additional endpoint is available:

```text
GET /indexes
```

All API responses are returned as JSON.

---

## 34. API Usage Examples

### Health

```http
GET /health
```

Example response:

```json
{
  "status": "HEALTHY",
  "api": "UP",
  "mongodb": "UP",
  "database": "midterm_data_pipeline"
}
```

### High Value Orders

```text
GET /queries/high_value_orders?min_amount=500000&limit=3
```

### Aggregation Report

```text
GET /aggregations/orders_by_status?limit=10
```

### Refresh Materialized Views

```http
POST /refresh-mv
```

Body:

```json
{
  "view_name": null,
  "force_rebuild": false
}
```

### Run Scheduled Job Manually

```text
POST /jobs/refresh_materialized_views/run
```

### Ingest CSV

```http
POST /ingest
```

Body example:

```json
{
  "input_path": "data/orders_small_sample.csv"
}
```

The automatic Router decides between Python Batch and PySpark according to the input file size.

---

## 35. API Verification

The Unified FastAPI was tested using Swagger UI.

```text
GET  /health                  PASS
POST /ingest                  PASS
POST /indexes                 PASS
GET  /queries                 PASS
GET  /queries/{name}          PASS
GET  /aggregations            PASS
GET  /aggregations/{name}     PASS
POST /refresh-mv              PASS
GET  /jobs                    PASS
POST /jobs/{name}/run         PASS
Swagger /docs                 PASS
```

The `/ingest` endpoint was tested using a 500-row CSV and successfully executed the existing Phase 1 Python Batch pipeline.

The response included:

```text
run_id
route
raw_metrics
elt_metrics
results
```

---

## 36. Environment Configuration

An example configuration file is provided:

```text
.env.example
```

It documents the configurable environment variables used by the project:

```text
INPUT_FILE
SMALL_SAMPLE_FILE
SMALL_FILE_THRESHOLD_MB
BATCH_SIZE
SAMPLE_ROWS
MONGO_URI
MONGO_DB_NAME
RAW_COLLECTION
VALIDATED_COLLECTION
QUARANTINE_COLLECTION
SPARK_MASTER
SPARK_APP_NAME
```

The project also contains safe default values in `config/settings.py`.

No passwords or private secrets are stored in `.env.example`.

---

## 37. Requirements

Install the Python dependencies:

```powershell
pip install -r requirements.txt
```

The current requirements include:

```text
pymongo==4.17.0
pyspark==4.2.0
pytest==9.1.1
fastapi==0.127.0
uvicorn==0.40.0
```

MongoDB Server and Java are also required for the MongoDB and PySpark parts of the project.

---

## 38. Phase 2 Final Status

```text
Practical Queries                    PASS
Indexes                              PASS
Compound Indexes                     PASS
Explain Before/After                 PASS
Aggregation Reports                  PASS
Materialized Views                   PASS
Incremental Refresh                  PASS
Scheduled Jobs                       PASS
Job Logs                             PASS
Unified FastAPI                      PASS
Swagger UI                           PASS
Required API Endpoints               PASS
requirements.txt                     PASS
.env.example                         PASS
```

Phase 2 was implemented as an extension of the same repository and the same Hybrid Big Data ELT Pipeline.

The original Midterm pipeline remains intact.