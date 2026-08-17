from collections import Counter

from pymongo import MongoClient

from spark_elt_pipeline import (
    create_spark_session,
    read_raw_run,
    apply_spark_quality_rules,
)


RUN_ID = "3df8788d-5da7-4dee-abad-0657f14b36c4"
LIMIT = 10000

MONGO_URI = "mongodb://127.0.0.1:27017"
DB_NAME = "midterm_data_pipeline"


def main():
    # =====================================================
    # PYTHON RESULTS FROM MONGODB
    # =====================================================

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    python_status = {}

    validated_cursor = db.orders_validated.find(
        {
            "run_id": RUN_ID,
            "source_row_number": {
                "$gte": 2,
                "$lte": 10001,
            },
        },
        {
            "_id": 0,
            "source_row_number": 1,
            "quality_status": 1,
        },
    )

    for doc in validated_cursor:
        python_status[doc["source_row_number"]] = {
            "status": doc["quality_status"],
            "reasons": [],
        }

    quarantine_cursor = db.orders_quarantine.find(
        {
            "run_id": RUN_ID,
            "source_row_number": {
                "$gte": 2,
                "$lte": 10001,
            },
        },
        {
            "_id": 0,
            "source_row_number": 1,
            "reason_codes": 1,
        },
    )

    for doc in quarantine_cursor:
        python_status[doc["source_row_number"]] = {
            "status": "Quarantined",
            "reasons": doc.get("reason_codes", []),
        }

    client.close()

    # =====================================================
    # SPARK RESULTS
    # =====================================================

    spark = create_spark_session()

    try:
        raw_df = read_raw_run(
            spark,
            RUN_ID,
            limit=LIMIT,
        )

        classified_df = apply_spark_quality_rules(
            raw_df
        )

        spark_rows = (
            classified_df
            .select(
                "source_row_number",
                "quality_status",
                "reason_codes",
            )
            .collect()
        )

        # =================================================
        # COMPARE
        # =================================================

        mismatch_types = Counter()
        spark_extra_quarantine_reasons = Counter()

        mismatch_count = 0

        print()
        print("===== PYTHON VS SPARK 10K =====")

        for row in spark_rows:
            row_number = row["source_row_number"]

            spark_status = row["quality_status"]
            spark_reasons = row["reason_codes"] or []

            python_data = python_status.get(row_number)

            if python_data is None:
                continue

            python_result = python_data["status"]

            if python_result != spark_status:
                mismatch_count += 1

                key = (
                    f"{python_result} -> "
                    f"{spark_status}"
                )

                mismatch_types[key] += 1

                if (
                    spark_status == "Quarantined"
                    and python_result != "Quarantined"
                ):
                    for reason in spark_reasons:
                        spark_extra_quarantine_reasons[
                            reason
                        ] += 1

        print(
            f"Compared records : {len(spark_rows)}"
        )

        print(
            f"Mismatches       : {mismatch_count}"
        )

        print()
        print("===== STATUS DIFFERENCES =====")

        for key, count in mismatch_types.items():
            print(f"{key}: {count}")

        print()
        print(
            "===== SPARK EXTRA QUARANTINE REASONS ====="
        )

        for reason, count in (
            spark_extra_quarantine_reasons
            .most_common()
        ):
            print(f"{reason}: {count}")

    finally:
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()