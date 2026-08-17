from pymongo import MongoClient
from pyspark.sql import functions as F

from spark_elt_pipeline import (
    create_spark_session,
    read_raw_run,
    apply_spark_quality_rules,
    build_validated_output,
)

from config.settings import (
    MONGO_URI,
    MONGO_DB_NAME,
)


RUN_ID = "3df8788d-5da7-4dee-abad-0657f14b36c4"

RAW_LIMIT = 1000
WRITE_LIMIT = 500

TEMP_COLLECTION = "orders_validated_smoke_test"


def write_temp(dataframe):

    (
        dataframe.write
        .format("mongodb")
        .mode("append")

        .option(
            "database",
            MONGO_DB_NAME,
        )

        .option(
            "collection",
            TEMP_COLLECTION,
        )

        .option(
            "operationType",
            "replace",
        )

        .option(
            "idFieldList",
            "order_id",
        )

        .option(
            "upsertDocument",
            "true",
        )

        .option(
            "ordered",
            "false",
        )

        .option(
            "maxBatchSize",
            "128",
        )

        .save()
    )


def main():

    client = MongoClient(
        MONGO_URI
    )

    db = client[
        MONGO_DB_NAME
    ]

    # حذف أي اختبار قديم فقط
    db[
        TEMP_COLLECTION
    ].drop()

    # Unique Business Key
    db[
        TEMP_COLLECTION
    ].create_index(
        "order_id",
        unique=True,
        name="ux_smoke_order_id",
    )

    spark = None

    try:

        spark = create_spark_session()

        print(
            "===== SPARK WRITE SMOKE TEST ====="
        )

        print(
            f"Run ID          : {RUN_ID}"
        )

        print(
            f"Temp Collection : {TEMP_COLLECTION}"
        )

        # =============================================
        # READ SMALL RAW SAMPLE
        # =============================================

        raw_df = read_raw_run(
            spark,
            RUN_ID,
            limit=RAW_LIMIT,
        )

        # =============================================
        # CLEAN + CLASSIFY
        # =============================================

        classified_df = (
            apply_spark_quality_rules(
                raw_df
            )
        )

        # Valid + Corrected only
        validated_df = (
            build_validated_output(
                classified_df
            )
            .limit(
                WRITE_LIMIT
            )
        )

        expected_count = (
            validated_df.count()
        )

        print(
            f"Records to test  : {expected_count}"
        )

        if expected_count == 0:
            raise RuntimeError(
                "No validated records available."
            )

        # =============================================
        # FIRST WRITE
        # =============================================

        first_df = (
            validated_df
            .withColumn(
                "smoke_version",
                F.lit("v1"),
            )
        )

        print()
        print(
            "First Spark write..."
        )

        write_temp(
            first_df
        )

        first_count = (
            db[
                TEMP_COLLECTION
            ].count_documents({})
        )

        print(
            f"Count after first write : {first_count}"
        )

        # =============================================
        # SECOND WRITE — SAME ORDER IDS
        # =============================================

        second_df = (
            validated_df
            .withColumn(
                "smoke_version",
                F.lit("v2"),
            )
        )

        print()
        print(
            "Second Spark write / Upsert..."
        )

        write_temp(
            second_df
        )

        second_count = (
            db[
                TEMP_COLLECTION
            ].count_documents({})
        )

        updated_count = (
            db[
                TEMP_COLLECTION
            ].count_documents(
                {
                    "smoke_version": "v2"
                }
            )
        )

        print(
            f"Count after second write: {second_count}"
        )

        print(
            f"Updated to v2           : {updated_count}"
        )

        # =============================================
        # VERIFY UNIQUE INDEX
        # =============================================

        indexes = list(
            db[
                TEMP_COLLECTION
            ].list_indexes()
        )

        unique_index_ok = any(
            index.get("name")
            == "ux_smoke_order_id"
            and index.get("unique")
            is True
            for index in indexes
        )

        print(
            f"Unique order_id index   : {unique_index_ok}"
        )

        # =============================================
        # FINAL CHECK
        # =============================================

        passed = (
            first_count
            == expected_count
            and second_count
            == expected_count
            and updated_count
            == expected_count
            and unique_index_ok
        )

        print()

        if passed:

            print(
                "SPARK WRITE SMOKE TEST: PASS"
            )

            print(
                "UPSERT / IDEMPOTENCY: PASS"
            )

        else:

            print(
                "SPARK WRITE SMOKE TEST: FAIL"
            )

            raise RuntimeError(
                "Spark MongoDB Upsert test failed."
            )

    finally:

        if spark is not None:

            try:
                spark.stop()

            except Exception:
                pass

        client.close()


if __name__ == "__main__":
    main()