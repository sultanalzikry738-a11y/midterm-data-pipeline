# Hybrid Big Data ELT Pipeline — Final Results

## 1. Project Overview

This project implements a **Hybrid Big Data ELT Pipeline** using:

* Python Batch Processing
* PySpark
* MongoDB
* Automatic file-size-based routing
* Raw-First ELT architecture
* Data Quality Rules
* Record Classification
* MongoDB Schema Validation
* Upsert and Idempotency

The pipeline automatically selects the processing engine according to the input file size.

---

## 2. Automatic Routing

Routing threshold:

**200 MB**

Routing rules:

| File Size | Selected Engine |
| --------- | --------------- |
| <= 200 MB | Python Batch    |
| > 200 MB  | PySpark         |

### Router Test Results

| Input File                    |        Size | Selected Engine | Result |
| ----------------------------- | ----------: | --------------- | ------ |
| orders_small_sample.csv       |    41.77 MB | python_batch    | PASS   |
| orders_huge_mixed_quality.csv | 12650.32 MB | pyspark         | PASS   |

**HYBRID ROUTER: PASS**

---

## 3. Data Source

Large input file:

`orders_huge_mixed_quality.csv`

File size:

**12650.32 MB (~12.35 GB)**

Total input records:

**30,000,000**

---

## 4. MongoDB Configuration

MongoDB connection:

`mongodb://127.0.0.1:27017`

Database:

`midterm_data_pipeline`

Collections:

* `orders_raw`
* `orders_validated`
* `orders_quarantine`

MongoDB Schema Validation was configured.

A Unique Index was created on:

`order_id`

Upsert and idempotency behavior were successfully verified.

---

## 5. Raw-First ELT Architecture

Every source record is first stored in raw form before quality processing.

Raw metadata fields:

* `run_id`
* `source_file`
* `source_row_number`
* `ingested_at`
* `engine_used`
* `raw_record`

This preserves the original source data and provides traceability for every ingested record.

---

## 6. Quality Classification

After applying the quality rules, every record is classified into one of three categories:

### Valid

The record is already valid and does not require correction.

### Corrected

The record contains a safely correctable issue and is corrected before being written to the validated collection.

### Quarantined

The record contains an unsafe, ambiguous, or impossible issue and is written to the quarantine collection for investigation.

---

## 7. Python Quality Rule Tests

Cleaning tests:

**6 PASS**

Classification tests:

**5 PASS**

Total:

**11 PASS**

---

## 8. Python End-to-End Test

Python Batch was tested on **100,000 records**.

| Classification |   Count |
| -------------- | ------: |
| Valid          |  74,859 |
| Corrected      |  17,108 |
| Quarantined    |   8,033 |
| Total          | 100,000 |

**CONSISTENCY: PASS**

---

## 9. Python vs PySpark Consistency Test

Python and PySpark were executed on the same **10,000 records**.

### Python Results

| Classification |  Count |
| -------------- | -----: |
| Valid          |  7,386 |
| Corrected      |  1,731 |
| Quarantined    |    883 |
| Total          | 10,000 |

### PySpark Results

| Classification |  Count |
| -------------- | -----: |
| Valid          |  7,386 |
| Corrected      |  1,731 |
| Quarantined    |    883 |
| Total          | 10,000 |

The Python and PySpark implementations produced identical classification counts.

**PYTHON vs SPARK COMPARISON: PASS**

---

## 10. Spark MongoDB Write Smoke Test

The PySpark MongoDB write path was tested using **500 records**.

Results:

| Test               | Result |
| ------------------ | -----: |
| First write count  |    500 |
| Second write count |    500 |
| Updated to v2      |    500 |
| Unique order_id    |   True |

The second write did not create duplicate records.

Existing records were correctly updated through upsert behavior.

**SPARK WRITE SMOKE TEST: PASS**

**UPSERT / IDEMPOTENCY: PASS**

---

## 11. Spark Dry Run

Before processing the complete dataset, a Spark dry run was performed on **1,000,000 records**.

| Classification |     Count |
| -------------- | --------: |
| Valid          |   751,678 |
| Corrected      |   170,671 |
| Quarantined    |    77,651 |
| Total          | 1,000,000 |

**DRY RUN: PASS**

---

## 12. Full Spark End-to-End ELT

The complete **30,000,000-record dataset** was successfully processed using PySpark.

### Execution Information

Start:

**August 17, 2026 — 3:52:26 PM**

End:

**August 17, 2026 — 9:17:12 PM**

Total duration:

**05:24:46**

### Final Results

| Output Category |      Count |
| --------------- | ---------: |
| Raw             | 30,000,000 |
| Valid           | 22,466,172 |
| Corrected       |  5,108,915 |
| Quarantined     |  2,424,913 |
| Total Output    | 30,000,000 |

Validated records consist of:

**Valid + Corrected**

`22,466,172 + 5,108,915 = 27,575,087`

MongoDB validated collection total:

**27,575,087**

Final consistency equation:

`22,466,172 + 5,108,915 + 2,424,913 = 30,000,000`

**CONSISTENCY: PASS**

**SPARK END-TO-END ELT: PASS**

---

## 13. Error Case Counts

The following error cases were detected during the full 30-million-record Spark execution:

| Error Case               |   Count |
| ------------------------ | ------: |
| UNKNOWN_PRICE            | 543,320 |
| CORRUPTED_ITEMS_JSON     | 419,906 |
| MISSING_CUSTOMER_ID      | 419,474 |
| AMBIGUOUS_NEGATIVE_VALUE | 419,135 |
| EMAIL_INVALID_UNSAFE     | 418,709 |
| INVALID_IMPOSSIBLE_DATE  | 210,524 |
| EMPTY_ITEMS              | 209,934 |
| DUPLICATE_ORDER_ID       | 209,895 |
| MISSING_ORDER_ID         | 209,392 |

The sum of error-case counts can be greater than the quarantine record count because a single quarantined record may contain more than one quality issue.

---

## 14. Spark Configuration

Spark version:

**4.2.0**

Spark master:

`local[4]`

Driver memory:

`8g`

Whole-stage code generation was disabled to prevent JVM code-generation errors caused by excessively large generated methods.

---

## 15. Run IDs

### Python Small Run

`3df8788d-5da7-4dee-abad-0657f14b36c4`

### Spark Big Run

`9b99b92b-d197-4b1f-9bc1-658037ec063d`

These run IDs provide traceability between processing runs and records stored in MongoDB.

---

## 16. Idempotency

The pipeline uses MongoDB upsert behavior together with a unique `order_id` index.

Repeated ingestion of the same logical records does not create duplicate validated records.

Integration testing confirmed:

**IDEMPOTENCY INTEGRATION TEST: PASS**

---

## 17. Final Verification Summary

| Component                        | Result |
| -------------------------------- | ------ |
| Small File Router → Python Batch | PASS   |
| Large File Router → PySpark      | PASS   |
| Python Cleaning Tests            | PASS   |
| Python Classification Tests      | PASS   |
| Python End-to-End                | PASS   |
| Python vs PySpark Comparison     | PASS   |
| Spark Write Smoke Test           | PASS   |
| Upsert / Idempotency             | PASS   |
| Spark 1M Dry Run                 | PASS   |
| Spark 30M End-to-End ELT         | PASS   |
| Final Record Consistency         | PASS   |
| `results.json` Validation        | PASS   |

---

## 18. Final Status

The Hybrid Big Data ELT Pipeline successfully demonstrates:

* Automatic routing between Python Batch and PySpark
* Large-scale processing of 30,000,000 records
* Raw-first ingestion
* MongoDB persistence
* Data cleaning and quality validation
* Valid, Corrected, and Quarantined classification
* Python and Spark behavioral consistency
* MongoDB schema validation
* Unique key enforcement
* Upsert behavior
* Idempotent processing
* Data lineage through run metadata
* Final reconciliation between input and output records

### Final Result

**HYBRID BIG DATA ELT PIPELINE: PASS**

**30,000,000 RECORDS PROCESSED SUCCESSFULLY**

**FINAL CONSISTENCY: PASS**
