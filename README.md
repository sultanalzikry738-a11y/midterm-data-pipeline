# Hybrid Big Data ELT Pipeline

## Midterm Data Pipeline Project

An end-to-end Hybrid ELT Data Pipeline for processing a large, dirty e-commerce orders dataset using:

- Python Batch Processing
- Apache Spark / PySpark
- MongoDB
- MongoDB Spark Connector

The pipeline automatically selects the processing engine based on the input file size.

---

# 1. Project Objective

The project builds a reliable ELT pipeline that:

1. Automatically detects the input file size.
2. Routes small files to Python Batch Processing.
3. Routes large files to PySpark.
4. Loads all records into MongoDB Raw storage before cleaning.
5. Applies deterministic data-quality rules.
6. Classifies records into:
   - Valid
   - Corrected
   - Quarantined
7. Stores corrected records with an Audit Trail.
8. Stores rejected records with explicit reason codes.
9. Uses MongoDB Upsert with a unique business key.
10. Verifies consistency and idempotency.

---

# 2. Hybrid Router

The routing threshold is configurable.

Default threshold:

```text
200 MB