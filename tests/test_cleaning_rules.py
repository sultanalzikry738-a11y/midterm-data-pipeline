from src.quality_rules import apply_quality_rules


# =========================================================
# TEST 1 — ARABIC NUMBER + DATE
# =========================================================

def test_arabic_number_and_date():

    raw = {
        "order_id": "طلب-1",
        "customer_id": "عميل-1",
        "order_date": "17-01-2025 04:50:00",
        "delivery_cost": "٥٠٠٠٫٠",
        "payment_amount": "٧٠٦٠٠٠٫٠",
        "total_amount": "٧٠٦٠٠٠٫٠",
        "currency": "YER",
        "customer_phone": "٧٧١٢٣٤٥٦٧",
        "customer_email": "user@example.com",
        "payment_status": "تم الدفع",
        "items_json": (
            '[{"sku":"SKU-1","qty":1,'
            '"unit_price":701000.0,'
            '"total":701000.0}]'
        ),
    }

    result = apply_quality_rules(raw)

    cleaned = result["cleaned_record"]

    assert cleaned["delivery_cost"] == 5000.0
    assert cleaned["payment_amount"] == 706000.0
    assert cleaned["total_amount"] == 706000.0

    assert (
        cleaned["order_date"]
        == "2025-01-17T04:50:00"
    )

    assert cleaned["customer_phone"] == "771234567"

    assert result["errors"] == []


# =========================================================
# TEST 2 — EMAIL + PHONE NORMALIZATION
# =========================================================

def test_email_and_phone_normalization():

    raw = {
        "order_id": "طلب-2",
        "customer_id": "عميل-2",
        "order_date": "2025-02-20T10:30:00",
        "delivery_cost": "5000",
        "payment_amount": "105000",
        "total_amount": "105000",
        "currency": "YER",
        "customer_phone": "+967 77 123 4567",
        "customer_email": "test@@example..com",
        "payment_status": "مدفوع",
        "items_json": (
            '[{"sku":"SKU-2","qty":1,'
            '"unit_price":100000.0,'
            '"total":100000.0}]'
        ),
    }

    result = apply_quality_rules(raw)

    cleaned = result["cleaned_record"]

    assert (
        cleaned["customer_phone"]
        == "+967771234567"
    )

    assert (
        cleaned["customer_email"]
        == "test@example.com"
    )

    assert (
        cleaned["payment_status"]
        == "تم الدفع"
    )

    assert result["errors"] == []

    assert len(result["corrections"]) > 0


# =========================================================
# TEST 3 — CORRUPTED JSON
# =========================================================

def test_corrupted_items_json():

    raw = {
        "order_id": "طلب-3",
        "customer_id": "عميل-3",
        "order_date": "2025-02-20T10:30:00",
        "delivery_cost": "5000",
        "payment_amount": "100000",
        "total_amount": "100000",
        "currency": "YER",
        "customer_phone": "771234567",
        "customer_email": "user@example.com",
        "payment_status": "تم الدفع",
        "items_json": "not-json",
    }

    result = apply_quality_rules(raw)

    error_codes = [
        error["code"]
        for error in result["errors"]
    ]

    assert (
        "CORRUPTED_ITEMS_JSON"
        in error_codes
    )


# =========================================================
# TEST 4 — NEGATIVE QUANTITY
# =========================================================

def test_negative_quantity():

    raw = {
        "order_id": "طلب-4",
        "customer_id": "عميل-4",
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

    result = apply_quality_rules(raw)

    error_codes = [
        error["code"]
        for error in result["errors"]
    ]

    assert (
        "AMBIGUOUS_NEGATIVE_VALUE"
        in error_codes
    )


# =========================================================
# TEST 5 — MISSING REQUIRED IDs
# =========================================================

def test_missing_required_ids():

    raw = {
        "order_id": "",
        "customer_id": "",
        "order_date": "2025-02-20T10:30:00",
        "delivery_cost": "5000",
        "payment_amount": "105000",
        "total_amount": "105000",
        "currency": "YER",
        "customer_phone": "771234567",
        "customer_email": "user@example.com",
        "payment_status": "تم الدفع",
        "items_json": (
            '[{"sku":"SKU-5","qty":1,'
            '"unit_price":100000.0,'
            '"total":100000.0}]'
        ),
    }

    result = apply_quality_rules(raw)

    error_codes = [
        error["code"]
        for error in result["errors"]
    ]

    assert "MISSING_ORDER_ID" in error_codes
    assert "MISSING_CUSTOMER_ID" in error_codes


# =========================================================
# TEST 6 — TOTAL RECALCULATION
# =========================================================

def test_total_recalculation():

    raw = {
        "order_id": "طلب-6",
        "customer_id": "عميل-6",
        "order_date": "2025-02-20T10:30:00",
        "delivery_cost": "5000",
        "payment_amount": "105000",
        "total_amount": "999999",
        "currency": "YER",
        "customer_phone": "771234567",
        "customer_email": "user@example.com",
        "payment_status": "تم الدفع",
        "items_json": (
            '[{"sku":"SKU-6","qty":1,'
            '"unit_price":100000.0,'
            '"total":100000.0}]'
        ),
    }

    result = apply_quality_rules(raw)

    cleaned = result["cleaned_record"]

    assert cleaned["total_amount"] == 105000.0

    rule_codes = [
        correction["rule_code"]
        for correction in result["corrections"]
    ]

    assert (
        "ORDER_TOTAL_RECALCULATED"
        in rule_codes
    )