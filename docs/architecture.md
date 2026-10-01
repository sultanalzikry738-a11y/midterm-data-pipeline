# Hybrid Big Data ELT Pipeline Architecture

## 1. نظرة عامة

المشروع عبارة عن خط بيانات هجين لمعالجة ملفات طلبات CSV.

يقوم النظام باختيار محرك المعالجة تلقائيًا حسب حجم الملف:

- Python Batch للملفات الصغيرة.
- PySpark للملفات الكبيرة.

بعد ذلك يتم تطبيق ELT بحيث تدخل جميع السجلات أولًا إلى MongoDB Raw قبل تنفيذ التنظيف أو التصنيف.

---

## 2. Architecture Flow

```text
Provided Dirty CSV
        |
        v
   File Router
        |
        | Check File Size
        |
   +----+--------------------+
   |                         |
   v                         v
Python Batch              PySpark
Small Files              Large Files
   |                         |
   +-----------+-------------+
               |
               v
          orders_raw
               |
               v
      Cleaning + Validation
               |
       +-------+--------+
       |                |
       v                v
Valid / Corrected   Quarantine
       |                |
       v                v
Idempotent Upsert   orders_quarantine
       |
       v
orders_validated
       |
       v
Metrics + Consistency
       |
       v
reports/results.json