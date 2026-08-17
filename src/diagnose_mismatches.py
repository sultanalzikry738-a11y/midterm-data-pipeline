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
    # READ PYTHON RESULTS
    # =====================================================

    client = MongoClient(MONGO_URI)

    db = client[DB_NAME]

    python_results = {}

    query = {
        "run_id": RUN_ID,
        "source_row_number": {
            "$gte": 2,
            "$lte": 10001,
        },
    }

    # Valid + Corrected
    for doc in db.orders_validated.find(
        query,
        {
            "_id": 0,
            "source_row_number": 1,
            "order_id": 1,
            "quality_status": 1,
            "corrections": 1,
        },
    ):

        python_results[
            doc["source_row_number"]
        ] = {
            "order_id": doc.get("order_id"),
            "status": doc.get("quality_status"),
            "corrections": doc.get(
                "corrections",
                [],
            ),
            "reasons": [],
        }

    # Quarantine
    for doc in db.orders_quarantine.find(
        query,
        {
            "_id": 0,
            "source_row_number": 1,
            "reason_codes": 1,
            "corrections": 1,
            "raw_record.order_id": 1,
        },
    ):

        python_results[
            doc["source_row_number"]
        ] = {
            "order_id": (
                doc.get(
                    "raw_record",
                    {},
                ).get(
                    "order_id"
                )
            ),
            "status": "Quarantined",
            "corrections": doc.get(
                "corrections",
                [],
            ),
            "reasons": doc.get(
                "reason_codes",
                [],
            ),
        }

    client.close()

    # =====================================================
    # READ SPARK RESULTS
    # =====================================================

    spark = create_spark_session()

    try:

        raw_df = read_raw_run(
            spark,
            RUN_ID,
            limit=LIMIT,
        )

        classified_df = (
            apply_spark_quality_rules(
                raw_df
            )
        )

        spark_rows = (
            classified_df
            .select(
                "source_row_number",
                "order_id",
                "quality_status",
                "corrections",
                "errors",
                "reason_codes",
            )
            .collect()
        )

        # =================================================
        # COUNTERS
        # =================================================

        missing_correction_rules = Counter()
        missing_correction_fields = Counter()

        spark_extra_errors = Counter()
        spark_unknown_price_fields = Counter()

        duplicate_extra = 0
        unknown_price_extra = 0

        corrected_to_valid = 0
        extra_quarantine = 0

        # =================================================
        # COMPARE
        # =================================================

        for row in spark_rows:

            row_number = row[
                "source_row_number"
            ]

            python_data = (
                python_results.get(
                    row_number
                )
            )

            if python_data is None:
                continue

            python_status = (
                python_data[
                    "status"
                ]
            )

            spark_status = (
                row[
                    "quality_status"
                ]
            )

            # =============================================
            # PYTHON CORRECTED BUT SPARK VALID
            # =============================================

            if (
                python_status == "Corrected"
                and spark_status == "Valid"
            ):

                corrected_to_valid += 1

                for correction in (
                    python_data[
                        "corrections"
                    ]
                ):

                    rule = correction.get(
                        "rule_code",
                        "UNKNOWN_RULE",
                    )

                    field = correction.get(
                        "field",
                        "UNKNOWN_FIELD",
                    )

                    missing_correction_rules[
                        rule
                    ] += 1

                    missing_correction_fields[
                        field
                    ] += 1

            # =============================================
            # SPARK EXTRA QUARANTINE
            # =============================================

            if (
                spark_status
                == "Quarantined"
                and
                python_status
                != "Quarantined"
            ):

                extra_quarantine += 1

                for error in (
                    row["errors"]
                    or []
                ):

                    code = error[
                        "code"
                    ]

                    field = error[
                        "field"
                    ]

                    spark_extra_errors[
                        f"{code} | {field}"
                    ] += 1

                    if (
                        code
                        == "DUPLICATE_ORDER_ID"
                    ):

                        duplicate_extra += 1

                    if (
                        code
                        == "UNKNOWN_PRICE"
                    ):

                        unknown_price_extra += 1

                        spark_unknown_price_fields[
                            field
                        ] += 1

        # =================================================
        # RESULTS
        # =================================================

        print()

        print(
            "===== DIAGNOSIS ====="
        )

        print(
            "Corrected -> Valid :",
            corrected_to_valid,
        )

        print(
            "Extra Quarantine   :",
            extra_quarantine,
        )

        print()

        print(
            "===== PYTHON CORRECTION RULES "
            "MISSING IN SPARK ====="
        )

        for (
            rule,
            count,
        ) in (
            missing_correction_rules
            .most_common()
        ):

            print(
                f"{rule}: {count}"
            )

        print()

        print(
            "===== CORRECTION FIELDS ====="
        )

        for (
            field,
            count,
        ) in (
            missing_correction_fields
            .most_common()
        ):

            print(
                f"{field}: {count}"
            )

        print()

        print(
            "===== SPARK EXTRA ERRORS ====="
        )

        for (
            error,
            count,
        ) in (
            spark_extra_errors
            .most_common()
        ):

            print(
                f"{error}: {count}"
            )

        print()

        print(
            "===== UNKNOWN PRICE FIELDS ====="
        )

        for (
            field,
            count,
        ) in (
            spark_unknown_price_fields
            .most_common()
        ):

            print(
                f"{field}: {count}"
            )

        print()

        print(
            "Duplicate extra    :",
            duplicate_extra,
        )

        print(
            "Unknown price extra:",
            unknown_price_extra,
        )

    finally:

        try:
            spark.stop()

        except Exception:
            pass


if __name__ == "__main__":
    main()