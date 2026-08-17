from src.classification import (
    CORRECTED,
    QUARANTINED,
    VALID,
    process_record,
)


# =========================================================
# VALID
# =========================================================

def test_valid_record():

    raw = {
        "order_id": "طلب-100",
        "customer_id": "عميل-100",
        "order_date": "2025-02-20T10:30:00",
        "delivery_cost": 5000.0,
        "payment_amount": 105000.0,
        "total_amount": 105000.0,
        "currency": "YER",
        "customer_phone": "771234567",
        "customer_email": "user@example.com",
        "payment_status": "تم الدفع",
        "items_json": (
            '[{"sku":"SKU-1","qty":1,'
            '"unit_price":100000.0,'
            '"total":100000.0}]'
        ),
    }

    result = process_record(
        raw
    )

    assert (
        result["classification"]
        == VALID
    )

    assert (
        result["corrections"]
        == []
    )

    assert (
        result["errors"]
        == []
    )


# =========================================================
# CORRECTED
# =========================================================

def test_corrected_record():

    raw = {
        "order_id": "طلب-101",
        "customer_id": "عميل-101",
        "order_date": "17-01-2025 04:50:00",
        "delivery_cost": "٥٠٠٠٫٠",
        "payment_amount": "١٠٥٠٠٠٫٠",
        "total_amount": "١٠٥٠٠٠٫٠",
        "currency": "ريال يمني",
        "customer_phone": "٧٧١٢٣٤٥٦٧",
        "customer_email": "user@example.com",
        "payment_status": "تم الدفع",
        "items_json": (
            '[{"sku":"SKU-2","qty":1,'
            '"unit_price":100000.0,'
            '"total":100000.0}]'
        ),
    }

    result = process_record(
        raw
    )

    assert (
        result["classification"]
        == CORRECTED
    )

    assert (
        len(
            result["corrections"]
        ) > 0
    )

    assert (
        result["errors"]
        == []
    )


# =========================================================
# QUARANTINED — CORRUPTED JSON
# =========================================================

def test_quarantined_corrupted_json():

    raw = {
        "order_id": "طلب-102",
        "customer_id": "عميل-102",
        "order_date": "2025-02-20T10:30:00",
        "delivery_cost": "5000",
        "payment_amount": "105000",
        "total_amount": "105000",
        "currency": "YER",
        "customer_phone": "771234567",
        "customer_email": "user@example.com",
        "payment_status": "تم الدفع",
        "items_json": "not-json",
    }

    result = process_record(
        raw
    )

    assert (
        result["classification"]
        == QUARANTINED
    )

    assert (
        "CORRUPTED_ITEMS_JSON"
        in result["reason_codes"]
    )


# =========================================================
# QUARANTINED — MISSING ORDER ID
# =========================================================

def test_quarantined_missing_order_id():

    raw = {
        "order_id": "",
        "customer_id": "عميل-103",
        "order_date": "2025-02-20T10:30:00",
        "delivery_cost": "5000",
        "payment_amount": "105000",
        "total_amount": "105000",
        "currency": "YER",
        "customer_phone": "771234567",
        "customer_email": "user@example.com",
        "payment_status": "تم الدفع",
        "items_json": (
            '[{"sku":"SKU-3","qty":1,'
            '"unit_price":100000.0,'
            '"total":100000.0}]'
        ),
    }

    result = process_record(
        raw
    )

    assert (
        result["classification"]
        == QUARANTINED
    )

    assert (
        "MISSING_ORDER_ID"
        in result["reason_codes"]
    )


# =========================================================
# QUARANTINED — NEGATIVE VALUE
# =========================================================

def test_quarantined_negative_quantity():

    raw = {
        "order_id": "طلب-104",
        "customer_id": "عميل-104",
        "order_date": "2025-02-20T10:30:00",
        "delivery_cost": "5000",
        "payment_amount": "105000",
        "total_amount": "105000",
        "currency": "YER",
        "customer_phone": "771234567",
        "customer_email": "user@example.com",
        "payment_status": "تم الدفع",
        "items_json": (
            '[{"sku":"SKU-4","qty":-2,'
            '"unit_price":50000.0,'
            '"total":100000.0}]'
        ),
    }

    result = process_record(
        raw
    )

    assert (
        result["classification"]
        == QUARANTINED
    )

    assert (
        "AMBIGUOUS_NEGATIVE_VALUE"
        in result["reason_codes"]
    )