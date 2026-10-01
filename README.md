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