from datetime import datetime, timezone

from src.elt_pipeline import (
    build_validated_document,
    upsert_validated_record,
)

from src.mongo_setup import (
    get_mongo_client,
    get_database,
    get_validated_collection,
)


TEST_ORDER_ID = (
    "__TEST_IDEMPOTENCY_ORDER__"
)


def make_raw_document():
    """
    Create a temporary Raw-like test document.
    """

    return {
        "run_id": "TEST-RUN-001",

        "source_file": "test.csv",

        "source_row_number": 2,

        "ingested_at": datetime.now(
            timezone.utc
        ),

        "engine_used": "python_batch",

        "raw_record": {},
    }


def make_classification_result(
    total_amount=105000.0,
):
    """
    Create a valid processed record.
    """

    return {
        "classification": "Valid",

        "cleaned_record": {
            "order_id": TEST_ORDER_ID,
            "customer_id": "TEST-CUSTOMER",
            "order_date": (
                "2025-01-01T10:00:00"
            ),
            "delivery_cost": 5000.0,
            "payment_amount": (
                total_amount
            ),
            "total_amount": (
                total_amount
            ),
            "currency": "YER",
            "customer_phone": "771234567",
            "customer_email": (
                "test@example.com"
            ),
            "payment_status": "تم الدفع",
            "status": "مؤكد",
            "items_json": (
                '[{"sku":"TEST-SKU",'
                '"qty":1,'
                '"unit_price":100000.0,'
                '"total":100000.0}]'
            ),
        },

        "corrections": [],

        "errors": [],

        "reason_codes": [],
    }


# =========================================================
# IDEMPOTENCY + UPDATE TEST
# =========================================================

def test_upsert_idempotency_and_update():

    client = get_mongo_client()

    try:

        database = get_database(
            client
        )

        collection = (
            get_validated_collection(
                database
            )
        )

        # تنظيف أي Test قديم
        collection.delete_many(
            {
                "order_id": TEST_ORDER_ID
            }
        )

        raw_document = (
            make_raw_document()
        )

        # =================================================
        # FIRST RUN
        # =================================================

        first_result = (
            make_classification_result(
                total_amount=105000.0
            )
        )

        first_document = (
            build_validated_document(
                raw_document,
                first_result,
            )
        )

        first_write = (
            upsert_validated_record(
                collection,
                first_document,
            )
        )

        assert (
            first_write["status"]
            == "inserted"
        )

        assert (
            collection.count_documents(
                {
                    "order_id":
                    TEST_ORDER_ID
                }
            )
            == 1
        )

        # =================================================
        # SAME INPUT AGAIN
        # =================================================

        second_write = (
            upsert_validated_record(
                collection,
                first_document,
            )
        )

        assert (
            second_write["status"]
            == "unchanged"
        )

        # يجب ألا يظهر Duplicate
        assert (
            collection.count_documents(
                {
                    "order_id":
                    TEST_ORDER_ID
                }
            )
            == 1
        )

        # =================================================
        # UPDATE EXISTING BUSINESS RECORD
        # =================================================

        updated_result = (
            make_classification_result(
                total_amount=110000.0
            )
        )

        updated_document = (
            build_validated_document(
                raw_document,
                updated_result,
            )
        )

        third_write = (
            upsert_validated_record(
                collection,
                updated_document,
            )
        )

        assert (
            third_write["status"]
            == "updated"
        )

        # ما زال Record واحد فقط
        assert (
            collection.count_documents(
                {
                    "order_id":
                    TEST_ORDER_ID
                }
            )
            == 1
        )

        saved = collection.find_one(
            {
                "order_id":
                TEST_ORDER_ID
            }
        )

        assert (
            saved["total_amount"]
            == 110000.0
        )

    finally:

        # لا نترك Test data في المشروع
        try:
            collection.delete_many(
                {
                    "order_id":
                    TEST_ORDER_ID
                }
            )
        except Exception:
            pass

        client.close()