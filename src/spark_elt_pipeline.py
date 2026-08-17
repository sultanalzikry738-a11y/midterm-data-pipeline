import argparse
import json
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# =========================================================
# PROJECT
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import (
    MONGO_URI,
    MONGO_DB_NAME,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)


# =========================================================
# CONSTANTS
# =========================================================

VALID = "Valid"
CORRECTED = "Corrected"
QUARANTINED = "Quarantined"

ARABIC_DIGITS = "٠١٢٣٤٥٦٧٨٩"
LATIN_DIGITS = "0123456789"


RAW_FIELDS = [
    "order_id",
    "order_date",
    "status",
    "customer_id",
    "customer_name",
    "customer_phone",
    "customer_email",
    "city",
    "district",
    "delivery_type",
    "delivery_cost",
    "payment_method",
    "payment_status",
    "payment_amount",
    "currency",
    "total_amount",
    "items_json",
]


NUMERIC_FIELDS = [
    "delivery_cost",
    "payment_amount",
    "total_amount",
]


KNOWN_PRICE_WORDS = {
    "ألف": "1000.0",
    "الف": "1000.0",
    "ألفان": "2000.0",
    "الفان": "2000.0",
    "خمسة آلاف": "5000.0",
    "خمسة الاف": "5000.0",
}


# =========================================================
# SCHEMAS
# =========================================================

RAW_RECORD_SCHEMA = StructType(
    [
        StructField(
            field,
            StringType(),
            True,
        )
        for field in RAW_FIELDS
    ]
)


RAW_MONGO_SCHEMA = StructType(
    [
        StructField(
            "run_id",
            StringType(),
            True,
        ),
        StructField(
            "source_file",
            StringType(),
            True,
        ),
        StructField(
            "source_row_number",
            LongType(),
            True,
        ),
        StructField(
            "ingested_at",
            TimestampType(),
            True,
        ),
        StructField(
            "engine_used",
            StringType(),
            True,
        ),
        StructField(
            "raw_record",
            RAW_RECORD_SCHEMA,
            True,
        ),
    ]
)


ITEM_SCHEMA = StructType(
    [
        StructField(
            "sku",
            StringType(),
            True,
        ),
        StructField(
            "name",
            StringType(),
            True,
        ),
        StructField(
            "qty",
            DoubleType(),
            True,
        ),
        StructField(
            "unit_price",
            DoubleType(),
            True,
        ),
        StructField(
            "total",
            DoubleType(),
            True,
        ),
    ]
)


ITEMS_SCHEMA = ArrayType(
    ITEM_SCHEMA
)


# =========================================================
# SPARK SESSION
# =========================================================

def create_spark_session() -> SparkSession:

    spark = (
        SparkSession.builder
        .master("local[4]")
        .appName(
            "MidtermSparkELTPipeline"
        )
        .config(
            "spark.mongodb.read.connection.uri",
            MONGO_URI,
        )
        .config(
            "spark.mongodb.write.connection.uri",
            MONGO_URI,
        )
        .config(
            "spark.sql.codegen.wholeStage",
            "false",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "64",
        )
        .config(
            "spark.default.parallelism",
            "4",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    return spark


# =========================================================
# READ RAW FROM MONGODB
# =========================================================

def read_raw_run(
    spark: SparkSession,
    run_id: str,
    limit: int | None = None,
):

    pipeline_steps = [
        {
            "$match": {
                "run_id": run_id,
            }
        }
    ]

    # في Dry Run يتم تنفيذ Limit داخل MongoDB.
    if (
        limit is not None
        and limit > 0
    ):
        pipeline_steps.append(
            {
                "$limit": int(limit)
            }
        )

    pipeline = json.dumps(
        pipeline_steps,
        ensure_ascii=False,
    )

    reader = (
        spark.read
        .format(
            "mongodb"
        )
        .option(
            "database",
            MONGO_DB_NAME,
        )
        .option(
            "collection",
            RAW_COLLECTION,
        )
        .option(
            "aggregation.pipeline",
            pipeline,
        )
        .option(
            "mode",
            "PERMISSIVE",
        )
        .schema(
            RAW_MONGO_SCHEMA
        )
    )

    # Partition واحدة للاختبارات الصغيرة.
    if (
        limit is not None
        and limit > 0
    ):

        reader = reader.option(
            "partitioner",
            (
                "com.mongodb.spark.sql.connector."
                "read.partitioner."
                "SinglePartitionPartitioner"
            ),
        )

    return reader.load()


# =========================================================
# AUDIT HELPERS
# =========================================================

def correction_struct(
    field: str,
    original_value,
    corrected_value,
    rule_code: str,
):

    return F.struct(
        F.lit(
            field
        ).alias(
            "field"
        ),

        original_value.cast(
            "string"
        ).alias(
            "original_value"
        ),

        corrected_value.cast(
            "string"
        ).alias(
            "corrected_value"
        ),

        F.lit(
            rule_code
        ).alias(
            "rule_code"
        ),
    )


def error_struct(
    code: str,
    field: str,
    value,
    message: str,
):

    return F.struct(
        F.lit(
            code
        ).alias(
            "code"
        ),

        F.lit(
            field
        ).alias(
            "field"
        ),

        value.cast(
            "string"
        ).alias(
            "value"
        ),

        F.lit(
            message
        ).alias(
            "message"
        ),
    )


# =========================================================
# NUMERIC NORMALIZATION
# =========================================================

def normalize_numeric_text(
    column,
):

    result = F.trim(
        column
    )

    # Arabic digits -> Latin digits
    result = F.translate(
        result,
        ARABIC_DIGITS,
        LATIN_DIGITS,
    )

    # Arabic decimal separator
    result = F.regexp_replace(
        result,
        "٫",
        ".",
    )

    # Thousands separators
    result = F.regexp_replace(
        result,
        "[,٬]",
        "",
    )

    # Known deterministic price words
    for (
        word,
        numeric_value,
    ) in KNOWN_PRICE_WORDS.items():

        result = F.when(
            result == word,
            F.lit(
                numeric_value
            ),
        ).otherwise(
            result
        )

    return result


# =========================================================
# QUALITY RULES
# =========================================================

def apply_spark_quality_rules(
    raw_dataframe,
):

    # =====================================================
    # FLATTEN RAW RECORD
    # =====================================================

    df = raw_dataframe.select(

        "run_id",
        "source_file",
        "source_row_number",
        "ingested_at",
        "engine_used",
        "raw_record",

        *[
            F.col(
                f"raw_record.{field}"
            ).alias(
                field
            )
            for field in RAW_FIELDS
        ],
    )


    # =====================================================
    # RULE 1 — WHITESPACE TRIM FOR ALL FIELDS
    # =====================================================

    for field in RAW_FIELDS:

        # Original value for Audit Trail
        df = df.withColumn(
            f"__original_{field}",
            F.col(
                field
            ),
        )

        # Trimmed value
        df = df.withColumn(
            f"__trimmed_{field}",
            F.trim(
                F.col(
                    field
                )
            ),
        )

        # Use trimmed value
        df = df.withColumn(
            field,
            F.col(
                f"__trimmed_{field}"
            ),
        )


    # =====================================================
    # NUMERIC NORMALIZATION
    # =====================================================

    for field in NUMERIC_FIELDS:

        df = df.withColumn(
            f"__numeric_text_{field}",
            normalize_numeric_text(
                F.col(
                    field
                )
            ),
        )

        df = df.withColumn(
            f"__numeric_{field}",
            F.expr(
                f"try_cast("
                f"`__numeric_text_{field}` "
                f"AS DOUBLE)"
            ),
        )


    df = df.withColumn(
        "delivery_cost",
        F.col(
            "__numeric_delivery_cost"
        ),
    )

    df = df.withColumn(
        "payment_amount",
        F.col(
            "__numeric_payment_amount"
        ),
    )

    df = df.withColumn(
        "total_amount",
        F.col(
            "__numeric_total_amount"
        ),
    )


    # =====================================================
    # CURRENCY NORMALIZATION
    # =====================================================

    currency_text = F.col(
        "currency"
    )

    df = df.withColumn(
        "currency",

        F.when(
            F.lower(
                currency_text
            ) == "yer",

            F.lit(
                "YER"
            ),
        )

        .when(
            currency_text.isin(
                "ريال",
                "ريال يمني",
                "ر.ي",
            ),

            F.lit(
                "YER"
            ),
        )

        .otherwise(
            currency_text
        ),
    )


    # =====================================================
    # PAYMENT STATUS NORMALIZATION
    # =====================================================

    payment_status_text = F.col(
        "payment_status"
    )

    df = df.withColumn(
        "payment_status",

        F.when(
            payment_status_text
            == "مدفوع",

            F.lit(
                "تم الدفع"
            ),
        )

        .otherwise(
            payment_status_text
        ),
    )


    # =====================================================
    # PHONE NORMALIZATION
    # =====================================================

    phone = F.col(
        "customer_phone"
    )

    phone = F.translate(
        phone,
        ARABIC_DIGITS,
        LATIN_DIGITS,
    )

    phone = F.regexp_replace(
        phone,
        r"[\s\-\(\)]",
        "",
    )


    # 00967 -> +967
    phone = F.when(

        phone.startswith(
            "00967"
        ),

        F.concat(
            F.lit(
                "+967"
            ),

            F.substring(
                phone,
                6,
                100,
            ),
        ),

    ).otherwise(
        phone
    )


    # 967 -> +967
    phone = F.when(

        phone.startswith(
            "967"
        ),

        F.concat(
            F.lit(
                "+"
            ),
            phone,
        ),

    ).otherwise(
        phone
    )


    df = df.withColumn(
        "customer_phone",
        phone,
    )


    # =====================================================
    # EMAIL NORMALIZATION
    # =====================================================

    email_original = F.col(
        "customer_email"
    )


    email_candidate = F.regexp_replace(

        F.regexp_replace(
            email_original,
            "@{2,}",
            "@",
        ),

        r"\.{2,}",
        ".",
    )


    email_pattern = (
        r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    )


    df = df.withColumn(
        "customer_email",

        F.when(
            email_candidate.rlike(
                email_pattern
            ),

            email_candidate,
        )

        .otherwise(
            email_original
        ),
    )


    # =====================================================
    # DATE NORMALIZATION
    # =====================================================

    date_text = F.translate(
        F.col(
            "order_date"
        ),
        ARABIC_DIGITS,
        LATIN_DIGITS,
    )


    date_formats = [
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd HH:mm:ss",
        "dd-MM-yyyy HH:mm:ss",
        "dd/MM/yyyy HH:mm:ss",
        "yyyy/MM/dd HH:mm:ss",
        "yyyy-MM-dd",
        "dd/MM/yyyy",
        "yyyy/MM/dd",
    ]


    parsed_dates = [

        F.try_to_timestamp(

            date_text,

            F.lit(
                date_format
            ),
        )

        for date_format
        in date_formats
    ]


    df = df.withColumn(
        "__parsed_order_date",

        F.coalesce(
            *parsed_dates
        ),
    )


    df = df.withColumn(
        "order_date",

        F.when(
            F.col(
                "__parsed_order_date"
            ).isNotNull(),

            F.date_format(
                F.col(
                    "__parsed_order_date"
                ),

                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )

        .otherwise(
            date_text
        ),
    )


    # =====================================================
    # ITEMS JSON
    # =====================================================

    df = df.withColumn(
        "__items",

        F.from_json(
            F.col(
                "items_json"
            ),
            ITEMS_SCHEMA,
        ),
    )


    # =====================================================
    # NEGATIVE ITEM QUANTITY
    # =====================================================

    df = df.withColumn(
        "__negative_item_qty",

        F.coalesce(

            F.exists(
                F.col(
                    "__items"
                ),

                lambda item:
                    item[
                        "qty"
                    ] < 0,
            ),

            F.lit(
                False
            ),
        ),
    )


    # =====================================================
    # UNKNOWN ITEM TOTAL
    # =====================================================

    df = df.withColumn(
        "__unknown_item_total",

        F.coalesce(

            F.exists(
                F.col(
                    "__items"
                ),

                lambda item:
                    item[
                        "total"
                    ].isNull(),
            ),

            F.lit(
                False
            ),
        ),
    )


    # =====================================================
    # NEGATIVE ITEM TOTAL
    # =====================================================

    df = df.withColumn(
        "__negative_item_total",

        F.coalesce(

            F.exists(
                F.col(
                    "__items"
                ),

                lambda item:
                    item[
                        "total"
                    ] < 0,
            ),

            F.lit(
                False
            ),
        ),
    )


    # =====================================================
    # ORDER TOTAL RECALCULATION
    # =====================================================

    items_present = (

        F.col(
            "__items"
        ).isNotNull()

        &

        (
            F.size(
                F.col(
                    "__items"
                )
            ) > 0
        )
    )


    df = df.withColumn(
        "__items_total_sum",

        F.when(

            items_present

            &

            (
                ~F.col(
                    "__unknown_item_total"
                )
            )

            &

            (
                ~F.col(
                    "__negative_item_total"
                )
            ),

            F.aggregate(

                F.col(
                    "__items"
                ),

                F.lit(
                    0.0
                ),

                lambda total, item:
                    total
                    +
                    item[
                        "total"
                    ],
            ),
        ),
    )


    df = df.withColumn(
        "__calculated_total",

        F.round(

            F.col(
                "__items_total_sum"
            )

            +

            F.col(
                "delivery_cost"
            ),

            2,
        ),
    )


    can_recalculate = (

        items_present

        &

        (
            ~F.col(
                "__unknown_item_total"
            )
        )

        &

        (
            ~F.col(
                "__negative_item_total"
            )
        )

        &

        (
            ~F.col(
                "__negative_item_qty"
            )
        )

        &

        F.col(
            "delivery_cost"
        ).isNotNull()

        &

        F.col(
            "total_amount"
        ).isNotNull()

        &

        F.col(
            "__calculated_total"
        ).isNotNull()
    )


    df = df.withColumn(
        "__total_recalculated",

        can_recalculate

        &

        (
            F.abs(

                F.col(
                    "total_amount"
                )

                -

                F.col(
                    "__calculated_total"
                )

            ) > 0.01
        ),
    )


    df = df.withColumn(
        "total_amount",

        F.when(
            F.col(
                "__total_recalculated"
            ),

            F.col(
                "__calculated_total"
            ),
        )

        .otherwise(
            F.col(
                "total_amount"
            )
        ),
    )


    # =====================================================
    # DUPLICATE ORDER ID
    # =====================================================
    #
    # Python policy:
    # First occurrence stays.
    # Later occurrences go to Quarantine.
    #
    # =====================================================

    valid_order_id = (

        F.col(
            "order_id"
        ).isNotNull()

        &

        (
            F.col(
                "order_id"
            ) != ""
        )
    )


    # Stable deterministic key
    df = df.withColumn(
        "__stable_row_key",

        F.sha2(

            F.concat_ws(

                "||",

                *[
                    F.coalesce(

                        F.col(
                            field
                        ).cast(
                            "string"
                        ),

                        F.lit(
                            "<NULL>"
                        ),
                    )

                    for field
                    in RAW_FIELDS
                ],
            ),

            256,
        ),
    )


    duplicate_window = (
        Window
        .partitionBy(
            "order_id"
        )
    )


    duplicate_rank_window = (

        Window
        .partitionBy(
            "order_id"
        )

        .orderBy(

            F.col(
                "source_row_number"
            ).asc_nulls_last(),

            F.col(
                "__stable_row_key"
            ).asc(),
        )
    )


    df = df.withColumn(
        "__duplicate_count",

        F.when(

            valid_order_id,

            F.count(
                F.lit(
                    1
                )
            ).over(
                duplicate_window
            ),
        )

        .otherwise(
            F.lit(
                1
            )
        ),
    )


    df = df.withColumn(
        "__duplicate_rank",

        F.when(

            valid_order_id,

            F.row_number().over(
                duplicate_rank_window
            ),
        )

        .otherwise(
            F.lit(
                1
            )
        ),
    )


    df = df.withColumn(
        "__is_duplicate",

        (
            F.col(
                "__duplicate_count"
            ) > 1
        )

        &

        (
            F.col(
                "__duplicate_rank"
            ) > 1
        ),
    )


    # =====================================================
    # CORRECTIONS AUDIT TRAIL
    # =====================================================

    correction_expressions = []


    # =====================================================
    # WHITESPACE CORRECTIONS
    # =====================================================

    for field in RAW_FIELDS:

        correction_expressions.append(

            F.when(

                F.col(
                    f"__original_{field}"
                ).isNotNull()

                &

                (
                    F.col(
                        f"__original_{field}"
                    )

                    !=

                    F.col(
                        f"__trimmed_{field}"
                    )
                ),

                correction_struct(

                    field,

                    F.col(
                        f"__original_{field}"
                    ),

                    F.col(
                        f"__trimmed_{field}"
                    ),

                    "WHITESPACE_TRIM",
                ),
            )
        )


    # =====================================================
    # NUMERIC TEXT CORRECTIONS
    # =====================================================

    for field in NUMERIC_FIELDS:

        correction_expressions.append(

            F.when(

                F.col(
                    f"__trimmed_{field}"
                ).isNotNull()

                &

                F.col(
                    f"__numeric_text_{field}"
                ).isNotNull()

                &

                (
                    F.col(
                        f"__trimmed_{field}"
                    )

                    !=

                    F.col(
                        f"__numeric_text_{field}"
                    )
                ),

                correction_struct(

                    field,

                    F.col(
                        f"__trimmed_{field}"
                    ),

                    F.col(
                        f"__numeric_text_{field}"
                    ),

                    "NUMERIC_TEXT_NORMALIZE",
                ),
            )
        )


    correction_expressions.extend(
        [

            # =================================================
            # CURRENCY
            # =================================================

            F.when(

                F.col(
                    "__trimmed_currency"
                ).isNotNull()

                &

                (
                    F.col(
                        "__trimmed_currency"
                    )

                    !=

                    F.col(
                        "currency"
                    )
                ),

                correction_struct(

                    "currency",

                    F.col(
                        "__trimmed_currency"
                    ),

                    F.col(
                        "currency"
                    ),

                    "CURRENCY_NORMALIZE_YER",
                ),
            ),


            # =================================================
            # PAYMENT STATUS
            # =================================================

            F.when(

                F.col(
                    "__trimmed_payment_status"
                ).isNotNull()

                &

                (
                    F.col(
                        "__trimmed_payment_status"
                    )

                    !=

                    F.col(
                        "payment_status"
                    )
                ),

                correction_struct(

                    "payment_status",

                    F.col(
                        "__trimmed_payment_status"
                    ),

                    F.col(
                        "payment_status"
                    ),

                    "FIXED_VALUE_ALIAS",
                ),
            ),


            # =================================================
            # PHONE
            # =================================================

            F.when(

                F.col(
                    "__trimmed_customer_phone"
                ).isNotNull()

                &

                (
                    F.col(
                        "__trimmed_customer_phone"
                    )

                    !=

                    F.col(
                        "customer_phone"
                    )
                ),

                correction_struct(

                    "customer_phone",

                    F.col(
                        "__trimmed_customer_phone"
                    ),

                    F.col(
                        "customer_phone"
                    ),

                    "PHONE_FORMAT_NORMALIZE",
                ),
            ),


            # =================================================
            # EMAIL
            # =================================================

            F.when(

                F.col(
                    "__trimmed_customer_email"
                ).isNotNull()

                &

                (
                    F.col(
                        "__trimmed_customer_email"
                    )

                    !=

                    F.col(
                        "customer_email"
                    )
                ),

                correction_struct(

                    "customer_email",

                    F.col(
                        "__trimmed_customer_email"
                    ),

                    F.col(
                        "customer_email"
                    ),

                    "EMAIL_REPEATED_SYMBOLS",
                ),
            ),


            # =================================================
            # DATE
            # =================================================

            F.when(

                F.col(
                    "__parsed_order_date"
                ).isNotNull()

                &

                (
                    F.col(
                        "__trimmed_order_date"
                    )

                    !=

                    F.col(
                        "order_date"
                    )
                ),

                correction_struct(

                    "order_date",

                    F.col(
                        "__trimmed_order_date"
                    ),

                    F.col(
                        "order_date"
                    ),

                    "DATE_STANDARDIZE_ISO",
                ),
            ),


            # =================================================
            # TOTAL RECALCULATION
            # =================================================

            F.when(

                F.col(
                    "__total_recalculated"
                ),

                correction_struct(

                    "total_amount",

                    F.col(
                        "__numeric_total_amount"
                    ),

                    F.col(
                        "total_amount"
                    ),

                    "ORDER_TOTAL_RECALCULATED",
                ),
            ),
        ]
    )


    df = df.withColumn(
        "corrections",

        F.filter(

            F.array(
                *correction_expressions
            ),

            lambda item:
                item.isNotNull(),
        ),
    )


    # =====================================================
    # ERRORS / QUARANTINE
    # =====================================================

    error_expressions = [

        # =================================================
        # MISSING ORDER ID
        # =================================================

        F.when(

            F.col(
                "order_id"
            ).isNull()

            |

            (
                F.col(
                    "order_id"
                ) == ""
            ),

            error_struct(

                "MISSING_ORDER_ID",

                "order_id",

                F.col(
                    "order_id"
                ),

                "Order ID is missing.",
            ),
        ),


        # =================================================
        # MISSING CUSTOMER ID
        # =================================================

        F.when(

            F.col(
                "customer_id"
            ).isNull()

            |

            (
                F.col(
                    "customer_id"
                ) == ""
            ),

            error_struct(

                "MISSING_CUSTOMER_ID",

                "customer_id",

                F.col(
                    "customer_id"
                ),

                "Customer ID is missing.",
            ),
        ),


        # =================================================
        # INVALID DATE
        # =================================================

        F.when(

            F.col(
                "__parsed_order_date"
            ).isNull(),

            error_struct(

                "INVALID_IMPOSSIBLE_DATE",

                "order_date",

                F.col(
                    "__trimmed_order_date"
                ),

                (
                    "Date is missing, "
                    "unsupported, or impossible."
                ),
            ),
        ),


        # =================================================
        # INVALID EMAIL
        # =================================================

        F.when(

            F.col(
                "customer_email"
            ).isNotNull()

            &

            (
                F.col(
                    "customer_email"
                ) != ""
            )

            &

            (
                ~F.col(
                    "customer_email"
                ).rlike(
                    email_pattern
                )
            ),

            error_struct(

                "EMAIL_INVALID_UNSAFE",

                "customer_email",

                F.col(
                    "customer_email"
                ),

                (
                    "Email is invalid and cannot "
                    "be safely corrected."
                ),
            ),
        ),
    ]


    # =====================================================
    # NUMERIC ERRORS
    # =====================================================

    for field in NUMERIC_FIELDS:

        # UNKNOWN PRICE
        error_expressions.append(

            F.when(

                F.col(
                    f"__trimmed_{field}"
                ).isNotNull()

                &

                (
                    F.col(
                        f"__trimmed_{field}"
                    ) != ""
                )

                &

                F.col(
                    f"__numeric_{field}"
                ).isNull(),

                error_struct(

                    "UNKNOWN_PRICE",

                    field,

                    F.col(
                        f"__trimmed_{field}"
                    ),

                    (
                        "Numeric value cannot "
                        "be safely determined."
                    ),
                ),
            )
        )


        # NEGATIVE VALUE
        error_expressions.append(

            F.when(

                F.col(
                    f"__numeric_{field}"
                ).isNotNull()

                &

                (
                    F.col(
                        f"__numeric_{field}"
                    ) < 0
                ),

                error_struct(

                    "AMBIGUOUS_NEGATIVE_VALUE",

                    field,

                    F.col(
                        f"__numeric_{field}"
                    ),

                    (
                        "Negative monetary value "
                        "cannot be safely interpreted."
                    ),
                ),
            )
        )


    error_expressions.extend(
        [

            # =================================================
            # CORRUPTED ITEMS JSON
            # =================================================

            F.when(

                F.col(
                    "items_json"
                ).isNotNull()

                &

                (
                    F.col(
                        "items_json"
                    ) != ""
                )

                &

                F.col(
                    "__items"
                ).isNull(),

                error_struct(

                    "CORRUPTED_ITEMS_JSON",

                    "items_json",

                    F.col(
                        "items_json"
                    ),

                    "items_json cannot be parsed.",
                ),
            ),


            # =================================================
            # EMPTY ITEMS
            # =================================================

            F.when(

                F.col(
                    "items_json"
                ).isNull()

                |

                (
                    F.col(
                        "items_json"
                    ) == ""
                )

                |

                (
                    F.col(
                        "__items"
                    ).isNotNull()

                    &

                    (
                        F.size(
                            F.col(
                                "__items"
                            )
                        ) == 0
                    )
                ),

                error_struct(

                    "EMPTY_ITEMS",

                    "items_json",

                    F.col(
                        "items_json"
                    ),

                    "Order has no usable items.",
                ),
            ),


            # =================================================
            # NEGATIVE ITEM QUANTITY
            # =================================================

            F.when(

                F.col(
                    "__negative_item_qty"
                ),

                error_struct(

                    "AMBIGUOUS_NEGATIVE_VALUE",

                    "items_json.qty",

                    F.lit(
                        "negative quantity detected"
                    ),

                    (
                        "Negative item quantity cannot "
                        "be safely interpreted."
                    ),
                ),
            ),


            # =================================================
            # NEGATIVE ITEM TOTAL
            # =================================================

            F.when(

                F.col(
                    "__negative_item_total"
                ),

                error_struct(

                    "AMBIGUOUS_NEGATIVE_VALUE",

                    "items_json.total",

                    F.lit(
                        "negative item total detected"
                    ),

                    (
                        "Negative item total cannot "
                        "be safely interpreted."
                    ),
                ),
            ),


            # =================================================
            # DUPLICATE ORDER
            # =================================================

            F.when(

                F.col(
                    "__is_duplicate"
                ),

                error_struct(

                    "DUPLICATE_ORDER_ID",

                    "order_id",

                    F.col(
                        "order_id"
                    ),

                    (
                        "Repeated business key "
                        "after the first occurrence."
                    ),
                ),
            ),
        ]
    )


    # =====================================================
    # BUILD ERRORS ARRAY
    # =====================================================

    df = df.withColumn(
        "errors",

        F.filter(

            F.array(
                *error_expressions
            ),

            lambda item:
                item.isNotNull(),
        ),
    )


    # =====================================================
    # REASON CODES
    # =====================================================

    df = df.withColumn(
        "reason_codes",

        F.array_distinct(

            F.transform(

                F.col(
                    "errors"
                ),

                lambda item:
                    item[
                        "code"
                    ],
            )
        ),
    )


    # =====================================================
    # CLASSIFICATION
    # =====================================================

    df = df.withColumn(
        "quality_status",

        F.when(

            F.size(
                F.col(
                    "errors"
                )
            ) > 0,

            F.lit(
                QUARANTINED
            ),
        )

        .when(

            F.size(
                F.col(
                    "corrections"
                )
            ) > 0,

            F.lit(
                CORRECTED
            ),
        )

        .otherwise(
            F.lit(
                VALID
            )
        ),
    )


    # =====================================================
    # CLEANED RECORD
    # =====================================================

    df = df.withColumn(
        "cleaned_record",

        F.struct(

            *[
                F.col(
                    field
                ).alias(
                    field
                )

                for field
                in RAW_FIELDS
            ]
        ),
    )


    return df


# =========================================================
# VALIDATED OUTPUT
# =========================================================

def build_validated_output(
    classified_dataframe,
):

    return (

        classified_dataframe

        .filter(

            F.col(
                "quality_status"
            ).isin(
                VALID,
                CORRECTED,
            )
        )

        .select(

            *[
                F.col(
                    field
                )

                for field
                in RAW_FIELDS
            ],

            "quality_status",
            "corrections",
            "run_id",
            "source_file",
            "source_row_number",
            "engine_used",

            F.col(
                "ingested_at"
            ).alias(
                "source_ingested_at"
            ),

            F.current_timestamp().alias(
                "validated_at"
            ),
        )
    )


# =========================================================
# QUARANTINE OUTPUT
# =========================================================

def build_quarantine_output(
    classified_dataframe,
):

    return (

        classified_dataframe

        .filter(

            F.col(
                "quality_status"
            )
            == QUARANTINED
        )

        .select(

            "run_id",
            "source_file",
            "source_row_number",
            "engine_used",

            F.col(
                "ingested_at"
            ).alias(
                "source_ingested_at"
            ),

            F.current_timestamp().alias(
                "quarantined_at"
            ),

            "raw_record",
            "cleaned_record",
            "corrections",
            "errors",
            "reason_codes",
        )
    )


# =========================================================
# WRITE VALIDATED
# =========================================================

def write_validated(
    dataframe,
):

    (

        dataframe.write

        .format(
            "mongodb"
        )

        .mode(
            "append"
        )

        .option(
            "database",
            MONGO_DB_NAME,
        )

        .option(
            "collection",
            VALIDATED_COLLECTION,
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
            "256",
        )

        .save()
    )


# =========================================================
# WRITE QUARANTINE
# =========================================================

def write_quarantine(
    dataframe,
):

    (

        dataframe.write

        .format(
            "mongodb"
        )

        .mode(
            "append"
        )

        .option(
            "database",
            MONGO_DB_NAME,
        )

        .option(
            "collection",
            QUARANTINE_COLLECTION,
        )

        .option(
            "operationType",
            "insert",
        )

        .option(
            "ordered",
            "false",
        )

        .option(
            "maxBatchSize",
            "256",
        )

        .save()
    )


# =========================================================
# PREVENT ACCIDENTAL SECOND RUN
# =========================================================

def ensure_run_not_processed(
    run_id: str,
):

    from src.mongo_setup import (
        get_mongo_client,
        get_database,
    )


    client = None


    try:

        client = get_mongo_client()

        database = get_database(
            client
        )


        validated_exists = (

            database[
                VALIDATED_COLLECTION
            ].find_one(

                {
                    "run_id": run_id
                },

                {
                    "_id": 1
                },
            )
        )


        quarantine_exists = (

            database[
                QUARANTINE_COLLECTION
            ].find_one(

                {
                    "run_id": run_id
                },

                {
                    "_id": 1
                },
            )
        )


        if (
            validated_exists is not None
            or quarantine_exists is not None
        ):

            raise RuntimeError(
                "This run already has ELT output. "
                "Do not run Spark ELT again "
                "without explicit cleanup."
            )


    finally:

        if client is not None:
            client.close()


# =========================================================
# DATABASE COUNTS
# =========================================================

def get_database_counts(
    run_id: str,
) -> dict:

    from src.mongo_setup import (
        get_mongo_client,
        get_database,
    )


    client = None


    try:

        client = get_mongo_client()

        database = get_database(
            client
        )


        raw_count = (
            database[
                RAW_COLLECTION
            ].count_documents(
                {
                    "run_id": run_id
                }
            )
        )


        valid_count = (
            database[
                VALIDATED_COLLECTION
            ].count_documents(
                {
                    "run_id": run_id,
                    "quality_status": VALID,
                }
            )
        )


        corrected_count = (
            database[
                VALIDATED_COLLECTION
            ].count_documents(
                {
                    "run_id": run_id,
                    "quality_status": CORRECTED,
                }
            )
        )


        quarantine_count = (
            database[
                QUARANTINE_COLLECTION
            ].count_documents(
                {
                    "run_id": run_id
                }
            )
        )


        return {
            "raw_count": raw_count,
            "valid_count": valid_count,
            "corrected_count": corrected_count,
            "quarantine_count": quarantine_count,
        }


    finally:

        if client is not None:
            client.close()


# =========================================================
# QUARANTINE REASON COUNTS
# =========================================================

def get_reason_counts(
    run_id: str,
) -> dict:

    from src.mongo_setup import (
        get_mongo_client,
        get_database,
    )


    client = None


    try:

        client = get_mongo_client()

        database = get_database(
            client
        )


        pipeline = [

            {
                "$match": {
                    "run_id": run_id
                }
            },

            {
                "$unwind":
                    "$reason_codes"
            },

            {
                "$group": {

                    "_id":
                        "$reason_codes",

                    "count": {
                        "$sum": 1
                    },
                }
            },

            {
                "$sort": {
                    "_id": 1
                }
            },
        ]


        return {

            row["_id"]:
                row["count"]

            for row in
            database[
                QUARANTINE_COLLECTION
            ].aggregate(
                pipeline
            )
        }


    finally:

        if client is not None:
            client.close()


# =========================================================
# DRY RUN
# =========================================================

def run_dry_test(
    classified_dataframe,
):

    print()

    print(
        "===== DRY RUN CLASSIFICATION ====="
    )


    rows = (

        classified_dataframe

        .groupBy(
            "quality_status"
        )

        .count()

        .collect()
    )


    counts = {

        row[
            "quality_status"
        ]:
            row[
                "count"
            ]

        for row
        in rows
    }


    valid = counts.get(
        VALID,
        0,
    )


    corrected = counts.get(
        CORRECTED,
        0,
    )


    quarantined = counts.get(
        QUARANTINED,
        0,
    )


    processed = (
        valid
        + corrected
        + quarantined
    )


    print(
        f"Processed   : {processed}"
    )

    print(
        f"Valid       : {valid}"
    )

    print(
        f"Corrected   : {corrected}"
    )

    print(
        f"Quarantined : {quarantined}"
    )


    if processed <= 0:

        raise RuntimeError(
            "Dry Run returned no records."
        )


    print(
        "DRY RUN: PASS"
    )


# =========================================================
# SPARK END-TO-END ELT
# =========================================================

def run_spark_elt(
    run_id: str,
    dry_run: bool = False,
    limit: int | None = None,
    show_plan: bool = False,
):

    if not run_id:

        raise ValueError(
            "run_id is required."
        )


    if (
        limit is not None
        and limit <= 0
    ):

        raise ValueError(
            "limit must be greater than 0."
        )


    if (
        limit is not None
        and not dry_run
    ):

        raise ValueError(
            "--limit is allowed only "
            "together with --dry-run."
        )


    spark = None

    start_time = (
        time.perf_counter()
    )


    try:

        spark = create_spark_session()


        print(
            "===== SPARK END-TO-END ELT ====="
        )

        print(
            f"Run ID        : {run_id}"
        )

        print(
            f"Spark version : "
            f"{spark.version}"
        )

        print(
            f"Spark master  : "
            f"{spark.sparkContext.master}"
        )

        print(
            f"Spark UI      : "
            f"{spark.sparkContext.uiWebUrl}"
        )

        print(
            "Driver memory : "
            f"{spark.sparkContext.getConf().get(
                'spark.driver.memory',
                'default'
            )}"
        )


        if limit is not None:

            print(
                f"TEST LIMIT    : {limit}"
            )


        # =================================================
        # READ RAW
        # =================================================

        raw_dataframe = read_raw_run(

            spark=spark,

            run_id=run_id,

            limit=limit,
        )


        input_partitions = (
            raw_dataframe.rdd.getNumPartitions()
        )


        print(
            f"Partitions    : "
            f"{input_partitions}"
        )


        # =================================================
        # APPLY QUALITY RULES
        # =================================================

        classified = (
            apply_spark_quality_rules(
                raw_dataframe
            )
        )


        # =================================================
        # OPTIONAL PLAN
        # =================================================

        if show_plan:

            print()

            print(
                "===== SPARK PLAN ====="
            )

            classified.select(
                "order_id",
                "quality_status",
            ).explain(
                mode="simple"
            )


        # =================================================
        # DRY RUN
        # =================================================

        if dry_run:

            run_dry_test(
                classified
            )

            return


        # =================================================
        # PROTECT EXISTING RUN
        # =================================================

        ensure_run_not_processed(
            run_id
        )


        # =================================================
        # BUILD OUTPUTS
        # =================================================

        validated_dataframe = (
            build_validated_output(
                classified
            )
        )


        quarantine_dataframe = (
            build_quarantine_output(
                classified
            )
        )


        # =================================================
        # WRITE VALIDATED
        # =================================================

        print()

        print(
            "Writing Valid + Corrected..."
        )


        write_validated(
            validated_dataframe
        )


        print(
            "Validated write: PASS"
        )


        # =================================================
        # WRITE QUARANTINE
        # =================================================

        print()

        print(
            "Writing Quarantine..."
        )


        write_quarantine(
            quarantine_dataframe
        )


        print(
            "Quarantine write: PASS"
        )


        # =================================================
        # DATABASE VERIFICATION
        # =================================================

        counts = get_database_counts(
            run_id
        )


        raw_count = counts[
            "raw_count"
        ]


        valid_count = counts[
            "valid_count"
        ]


        corrected_count = counts[
            "corrected_count"
        ]


        quarantine_count = counts[
            "quarantine_count"
        ]


        terminal_count = (
            valid_count
            + corrected_count
            + quarantine_count
        )


        consistency_pass = (
            raw_count
            == terminal_count
        )


        elapsed_seconds = (
            time.perf_counter()
            - start_time
        )


        throughput = (

            raw_count
            / elapsed_seconds

            if elapsed_seconds > 0

            else 0.0
        )


        reason_counts = (
            get_reason_counts(
                run_id
            )
        )


        # =================================================
        # FINAL RESULT
        # =================================================

        print()

        print(
            "===== SPARK ELT FINAL RESULT ====="
        )


        print(
            f"Run ID             : "
            f"{run_id}"
        )


        print(
            f"Raw count          : "
            f"{raw_count}"
        )


        print(
            f"Valid              : "
            f"{valid_count}"
        )


        print(
            f"Corrected          : "
            f"{corrected_count}"
        )


        print(
            f"Validated total    : "
            f"{valid_count + corrected_count}"
        )


        print(
            f"Quarantined        : "
            f"{quarantine_count}"
        )


        print(
            f"Input partitions   : "
            f"{input_partitions}"
        )


        print(
            f"Elapsed seconds    : "
            f"{elapsed_seconds:.2f}"
        )


        print(
            f"Throughput         : "
            f"{throughput:.2f} rows/sec"
        )


        print()

        print(
            "===== ERROR CASE COUNTS ====="
        )


        if reason_counts:

            for (
                reason,
                count,
            ) in reason_counts.items():

                print(
                    f"{reason}: {count}"
                )


        else:

            print(
                "No quarantine reasons."
            )


        print()


        print(
            "CONSISTENCY:",
            (
                "PASS"
                if consistency_pass
                else "FAIL"
            ),
        )


        if not consistency_pass:

            raise RuntimeError(
                "Spark ELT consistency "
                "check failed."
            )


        print(
            "SPARK END-TO-END ELT: PASS"
        )


    finally:

        if spark is not None:

            try:

                spark.stop()

            except Exception:

                pass


# =========================================================
# ARGUMENTS
# =========================================================

def parse_arguments():

    parser = argparse.ArgumentParser(

        description=(
            "Process an existing MongoDB Raw run "
            "using Spark DataFrame ELT."
        )
    )


    parser.add_argument(
        "--run-id",

        required=True,

        help=(
            "Raw run_id to process."
        ),
    )


    parser.add_argument(
        "--dry-run",

        action="store_true",

        help=(
            "Run classification without writing "
            "orders_validated or orders_quarantine."
        ),
    )


    parser.add_argument(
        "--limit",

        type=int,

        default=None,

        help=(
            "Dry Run only. "
            "Limit is pushed directly into MongoDB."
        ),
    )


    parser.add_argument(
        "--show-plan",

        action="store_true",

        help=(
            "Print a compact Spark plan."
        ),
    )


    return parser.parse_args()


# =========================================================
# MAIN
# =========================================================

def main():

    args = parse_arguments()


    try:

        run_spark_elt(

            run_id=args.run_id,

            dry_run=args.dry_run,

            limit=args.limit,

            show_plan=args.show_plan,
        )


    except Exception as error:

        print()

        print(
            "SPARK END-TO-END ELT: FAIL"
        )

        print(
            f"ERROR TYPE: "
            f"{type(error).__name__}"
        )

        print(
            f"ERROR: {error}"
        )

        raise


if __name__ == "__main__":
    main()