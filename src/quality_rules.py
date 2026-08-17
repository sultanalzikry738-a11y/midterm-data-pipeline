import json
import re
from copy import deepcopy
from datetime import datetime


# =========================================================
# CONSTANTS
# =========================================================

ARABIC_DIGITS = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩",
    "0123456789",
)


NUMERIC_FIELDS = [
    "delivery_cost",
    "payment_amount",
    "total_amount",
]


# قيم نصية نعرف معناها بشكل مؤكد فقط
KNOWN_PRICE_WORDS = {
    "ألف": 1000.0,
    "الف": 1000.0,
    "ألفان": 2000.0,
    "الفان": 2000.0,
    "خمسة آلاف": 5000.0,
    "خمسة الاف": 5000.0,
}


# مرادفات واضحة فقط
PAYMENT_STATUS_ALIASES = {
    "مدفوع": "تم الدفع",
}


CURRENCY_ALIASES = {
    "YER": "YER",
    "yer": "YER",
    "ريال": "YER",
    "ريال يمني": "YER",
    "ر.ي": "YER",
}


# =========================================================
# AUDIT TRAIL
# =========================================================

def add_correction(
    corrections: list,
    field: str,
    original_value,
    corrected_value,
    rule_code: str,
):
    """
    Add one correction to the audit trail.
    """

    if original_value == corrected_value:
        return

    corrections.append(
        {
            "field": field,
            "original_value": original_value,
            "corrected_value": corrected_value,
            "rule_code": rule_code,
        }
    )


# =========================================================
# RULE 1 — WHITESPACE
# =========================================================

def trim_whitespace(
    record: dict,
    corrections: list,
):
    """
    Remove leading/trailing whitespace.
    """

    for field, value in list(record.items()):

        if not isinstance(value, str):
            continue

        corrected = value.strip()

        if corrected != value:

            record[field] = corrected

            add_correction(
                corrections,
                field,
                value,
                corrected,
                "WHITESPACE_TRIM",
            )


# =========================================================
# RULE 2 — ARABIC DIGITS
# =========================================================

def normalize_arabic_digits(
    record: dict,
    corrections: list,
):
    """
    Convert Arabic digits to Latin digits
    in safe fields only.
    """

    fields = [
        "delivery_cost",
        "payment_amount",
        "total_amount",
        "customer_phone",
        "order_date",
    ]

    for field in fields:

        value = record.get(field)

        if not isinstance(value, str):
            continue

        corrected = value.translate(
            ARABIC_DIGITS
        )

        if corrected != value:

            record[field] = corrected

            add_correction(
                corrections,
                field,
                value,
                corrected,
                "ARABIC_DIGITS_TO_LATIN",
            )


# =========================================================
# RULE 3 — ARABIC DECIMAL SEPARATOR
# =========================================================

def normalize_decimal_separator(
    record: dict,
    corrections: list,
):
    """
    Example:
    ٧٠٦٠٠٠٫٠
    ->
    706000.0
    """

    for field in NUMERIC_FIELDS:

        value = record.get(field)

        if not isinstance(value, str):
            continue

        corrected = value.replace(
            "٫",
            ".",
        )

        if corrected != value:

            record[field] = corrected

            add_correction(
                corrections,
                field,
                value,
                corrected,
                "ARABIC_DECIMAL_SEPARATOR",
            )


# =========================================================
# RULE 4 — THOUSANDS SEPARATOR
# =========================================================

def remove_thousands_separators(
    record: dict,
    corrections: list,
):
    """
    Examples:

    125,000.00 -> 125000.00
    125٬000    -> 125000
    """

    for field in NUMERIC_FIELDS:

        value = record.get(field)

        if not isinstance(value, str):
            continue

        corrected = (
            value
            .replace(",", "")
            .replace("٬", "")
        )

        if corrected != value:

            record[field] = corrected

            add_correction(
                corrections,
                field,
                value,
                corrected,
                "THOUSANDS_SEPARATOR_REMOVE",
            )


# =========================================================
# RULE 5 — CURRENCY
# =========================================================

def normalize_currency(
    record: dict,
    corrections: list,
):
    """
    Normalize known Yemen Riyal values to YER.
    """

    value = record.get(
        "currency"
    )

    if not isinstance(value, str):
        return

    normalized = value.strip()

    if normalized in CURRENCY_ALIASES:

        corrected = (
            CURRENCY_ALIASES[
                normalized
            ]
        )

        if corrected != value:

            record["currency"] = corrected

            add_correction(
                corrections,
                "currency",
                value,
                corrected,
                "CURRENCY_NORMALIZE_YER",
            )


# =========================================================
# RULE 6 — KNOWN PRICE WORDS
# =========================================================

def convert_known_price_words(
    record: dict,
    corrections: list,
):
    """
    Convert only explicitly known price words.

    No guessing.
    """

    for field in NUMERIC_FIELDS:

        value = record.get(field)

        if not isinstance(value, str):
            continue

        normalized = value.strip()

        if normalized in KNOWN_PRICE_WORDS:

            corrected = (
                KNOWN_PRICE_WORDS[
                    normalized
                ]
            )

            record[field] = corrected

            add_correction(
                corrections,
                field,
                value,
                corrected,
                "PRICE_WORDS_KNOWN",
            )


# =========================================================
# RULE 7 — PHONE FORMAT
# =========================================================

def normalize_phone(
    record: dict,
    corrections: list,
):
    """
    Normalize obvious phone formatting.

    Example:

    +967 77 123 4567
    ->
    +967771234567
    """

    value = record.get(
        "customer_phone"
    )

    if not isinstance(value, str):
        return

    corrected = (
        value
        .replace(" ", "")
        .replace("-", "")
        .replace("(", "")
        .replace(")", "")
    )

    # 00967 -> +967
    if corrected.startswith(
        "00967"
    ):

        corrected = (
            "+967"
            + corrected[5:]
        )

    # 967... -> +967...
    if (
        corrected.startswith("967")
        and not corrected.startswith("+967")
    ):

        corrected = (
            "+"
            + corrected
        )

    if corrected != value:

        record[
            "customer_phone"
        ] = corrected

        add_correction(
            corrections,
            "customer_phone",
            value,
            corrected,
            "PHONE_FORMAT_NORMALIZE",
        )


# =========================================================
# RULE 8 — EMAIL
# =========================================================

def normalize_email(
    record: dict,
    corrections: list,
    errors: list,
):
    """
    Fix only obvious repeated symbols.

    Example:

    user@@mail..com
    ->
    user@mail.com
    """

    value = record.get(
        "customer_email"
    )

    if value is None:
        return

    if not isinstance(value, str):
        return

    if value == "":
        return

    corrected = value

    while "@@" in corrected:

        corrected = corrected.replace(
            "@@",
            "@",
        )

    while ".." in corrected:

        corrected = corrected.replace(
            "..",
            ".",
        )

    email_pattern = re.compile(
        r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    )

    # إذا كان التصحيح واضحًا
    if (
        corrected != value
        and email_pattern.match(
            corrected
        )
    ):

        record[
            "customer_email"
        ] = corrected

        add_correction(
            corrections,
            "customer_email",
            value,
            corrected,
            "EMAIL_REPEATED_SYMBOLS",
        )

    # إذا البريد غير صالح ولا يمكن إصلاحه بأمان
    elif not email_pattern.match(
        value
    ):

        errors.append(
            {
                "code": "EMAIL_INVALID_UNSAFE",
                "field": "customer_email",
                "value": value,
                "message": (
                    "Email is invalid and "
                    "cannot be safely corrected."
                ),
            }
        )


# =========================================================
# RULE 9 — DATE FORMAT
# =========================================================

def normalize_order_date(
    record: dict,
    corrections: list,
    errors: list,
):
    """
    Convert supported valid dates to ISO format.

    No fuzzy guessing.
    """

    value = record.get(
        "order_date"
    )

    if not isinstance(value, str):
        return

    if not value:

        errors.append(
            {
                "code": "INVALID_IMPOSSIBLE_DATE",
                "field": "order_date",
                "value": value,
                "message": (
                    "Order date is missing."
                ),
            }
        )

        return

    supported_formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%Y/%m/%d",
    ]

    parsed_date = None

    for date_format in supported_formats:

        try:

            parsed_date = datetime.strptime(
                value,
                date_format,
            )

            break

        except ValueError:
            continue

    if parsed_date is None:

        errors.append(
            {
                "code": "INVALID_IMPOSSIBLE_DATE",
                "field": "order_date",
                "value": value,
                "message": (
                    "Date format is unsupported "
                    "or date is impossible."
                ),
            }
        )

        return

    corrected = parsed_date.isoformat(
        timespec="seconds"
    )

    if corrected != value:

        record[
            "order_date"
        ] = corrected

        add_correction(
            corrections,
            "order_date",
            value,
            corrected,
            "DATE_STANDARDIZE_ISO",
        )


# =========================================================
# RULE 10 — FIXED VALUE ALIASES
# =========================================================

def normalize_fixed_values(
    record: dict,
    corrections: list,
):
    """
    Normalize explicitly known aliases only.
    """

    value = record.get(
        "payment_status"
    )

    if isinstance(value, str):

        if value in PAYMENT_STATUS_ALIASES:

            corrected = (
                PAYMENT_STATUS_ALIASES[
                    value
                ]
            )

            if corrected != value:

                record[
                    "payment_status"
                ] = corrected

                add_correction(
                    corrections,
                    "payment_status",
                    value,
                    corrected,
                    "FIXED_VALUE_ALIAS",
                )


# =========================================================
# RULE 11 — NUMERIC TYPE CONVERSION
# =========================================================

def convert_numeric_types(
    record: dict,
    corrections: list,
    errors: list,
):
    """
    Convert normal numeric strings to float.

    IMPORTANT:
    Normal type conversion is NOT considered
    a data-quality correction.

    Example:

    "5000.0" -> 5000.0

    This is only type normalization.

    Real repairs such as Arabic digits or
    separators were already recorded by
    previous rules.
    """

    for field in NUMERIC_FIELDS:

        value = record.get(field)

        if value is None:
            continue

        if isinstance(
            value,
            (int, float),
        ):
            continue

        if not isinstance(
            value,
            str,
        ):
            continue

        if value == "":
            continue

        try:

            record[field] = float(
                value
            )

        except ValueError:

            errors.append(
                {
                    "code": "UNKNOWN_PRICE",
                    "field": field,
                    "value": value,
                    "message": (
                        "Numeric value cannot "
                        "be safely determined."
                    ),
                }
            )


# =========================================================
# ITEMS JSON VALIDATION
# =========================================================

def validate_items_json(
    record: dict,
    errors: list,
):
    """
    Parse items_json and validate its structure.
    """

    value = record.get(
        "items_json"
    )

    if (
        value is None
        or value == ""
    ):

        errors.append(
            {
                "code": "EMPTY_ITEMS",
                "field": "items_json",
                "value": value,
                "message": (
                    "Order does not contain items."
                ),
            }
        )

        return None

    # إذا كانت بالفعل List
    if isinstance(
        value,
        list,
    ):

        items = value

    # إذا كانت String نحاول JSON parse
    elif isinstance(
        value,
        str,
    ):

        try:

            items = json.loads(
                value
            )

        except (
            json.JSONDecodeError,
            TypeError,
        ):

            errors.append(
                {
                    "code": "CORRUPTED_ITEMS_JSON",
                    "field": "items_json",
                    "value": value,
                    "message": (
                        "items_json cannot be parsed."
                    ),
                }
            )

            return None

    else:

        errors.append(
            {
                "code": "CORRUPTED_ITEMS_JSON",
                "field": "items_json",
                "value": value,
                "message": (
                    "items_json has unsupported type."
                ),
            }
        )

        return None

    # يجب أن يحتوي JSON على List
    if not isinstance(
        items,
        list,
    ):

        errors.append(
            {
                "code": "CORRUPTED_ITEMS_JSON",
                "field": "items_json",
                "value": value,
                "message": (
                    "items_json must contain a list."
                ),
            }
        )

        return None

    # لا نقبل قائمة فارغة
    if len(items) == 0:

        errors.append(
            {
                "code": "EMPTY_ITEMS",
                "field": "items_json",
                "value": value,
                "message": (
                    "Items list is empty."
                ),
            }
        )

        return None

    return items


# =========================================================
# REQUIRED IDS
# =========================================================

def validate_required_ids(
    record: dict,
    errors: list,
):
    """
    Validate essential business IDs.
    """

    order_id = record.get(
        "order_id"
    )

    customer_id = record.get(
        "customer_id"
    )

    if (
        order_id is None
        or str(order_id).strip() == ""
    ):

        errors.append(
            {
                "code": "MISSING_ORDER_ID",
                "field": "order_id",
                "value": order_id,
                "message": (
                    "Order ID is missing."
                ),
            }
        )

    if (
        customer_id is None
        or str(customer_id).strip() == ""
    ):

        errors.append(
            {
                "code": "MISSING_CUSTOMER_ID",
                "field": "customer_id",
                "value": customer_id,
                "message": (
                    "Customer ID is missing."
                ),
            }
        )


# =========================================================
# NEGATIVE VALUES
# =========================================================

def validate_negative_values(
    record: dict,
    items,
    errors: list,
):
    """
    Detect ambiguous negative monetary values
    and negative item quantities.
    """

    # Monetary fields
    for field in NUMERIC_FIELDS:

        value = record.get(
            field
        )

        if isinstance(
            value,
            (int, float),
        ):

            if value < 0:

                errors.append(
                    {
                        "code": "AMBIGUOUS_NEGATIVE_VALUE",
                        "field": field,
                        "value": value,
                        "message": (
                            "Negative monetary value "
                            "cannot be safely interpreted."
                        ),
                    }
                )

    # Items quantities
    if not items:
        return

    for index, item in enumerate(
        items
    ):

        if not isinstance(
            item,
            dict,
        ):
            continue

        qty = item.get(
            "qty"
        )

        if isinstance(
            qty,
            (int, float),
        ) and qty < 0:

            errors.append(
                {
                    "code": "AMBIGUOUS_NEGATIVE_VALUE",
                    "field": (
                        f"items_json[{index}].qty"
                    ),
                    "value": qty,
                    "message": (
                        "Negative item quantity "
                        "cannot be safely interpreted."
                    ),
                }
            )


# =========================================================
# RULE 12 — ORDER TOTAL
# =========================================================

def recalculate_order_total(
    record: dict,
    items,
    corrections: list,
):
    """
    Recalculate total_amount only when every
    required component is valid and available.
    """

    if not items:
        return

    delivery_cost = record.get(
        "delivery_cost"
    )

    current_total = record.get(
        "total_amount"
    )

    if not isinstance(
        delivery_cost,
        (int, float),
    ):
        return

    item_total_sum = 0.0

    for item in items:

        if not isinstance(
            item,
            dict,
        ):
            return

        item_total = item.get(
            "total"
        )

        if not isinstance(
            item_total,
            (int, float),
        ):
            return

        if item_total < 0:
            return

        item_total_sum += float(
            item_total
        )

    calculated_total = round(
        item_total_sum
        + float(delivery_cost),
        2,
    )

    if not isinstance(
        current_total,
        (int, float),
    ):
        return

    # إذا الإجمالي مختلف عن المحسوب
    if abs(
        float(current_total)
        - calculated_total
    ) > 0.01:

        original = current_total

        record[
            "total_amount"
        ] = calculated_total

        add_correction(
            corrections,
            "total_amount",
            original,
            calculated_total,
            "ORDER_TOTAL_RECALCULATED",
        )


# =========================================================
# MAIN QUALITY FUNCTION
# =========================================================

def apply_quality_rules(
    raw_record: dict,
) -> dict:
    """
    Apply deterministic cleaning and validation rules.

    Returns:

    cleaned_record
    corrections
    errors
    """

    # نسخة حتى لا نعدل Raw الأصلية
    record = deepcopy(
        raw_record
    )

    corrections = []

    errors = []

    # =====================================================
    # AUTOMATIC CORRECTION RULES
    # =====================================================

    trim_whitespace(
        record,
        corrections,
    )

    normalize_arabic_digits(
        record,
        corrections,
    )

    normalize_decimal_separator(
        record,
        corrections,
    )

    remove_thousands_separators(
        record,
        corrections,
    )

    normalize_currency(
        record,
        corrections,
    )

    convert_known_price_words(
        record,
        corrections,
    )

    normalize_phone(
        record,
        corrections,
    )

    normalize_email(
        record,
        corrections,
        errors,
    )

    normalize_order_date(
        record,
        corrections,
        errors,
    )

    normalize_fixed_values(
        record,
        corrections,
    )

    # تحويل النوع الطبيعي
    # لا يعتبر Correction
    convert_numeric_types(
        record,
        corrections,
        errors,
    )

    # =====================================================
    # VALIDATION RULES
    # =====================================================

    validate_required_ids(
        record,
        errors,
    )

    items = validate_items_json(
        record,
        errors,
    )

    validate_negative_values(
        record,
        items,
        errors,
    )

    # =====================================================
    # TOTAL VALIDATION / CORRECTION
    # =====================================================

    recalculate_order_total(
        record,
        items,
        corrections,
    )

    # =====================================================
    # RESULT
    # =====================================================

    return {
        "cleaned_record": record,
        "corrections": corrections,
        "errors": errors,
    }