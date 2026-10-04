# ============================================================
# Phase 2 - Materialized Views
# ============================================================
#
# هذا الملف ينشئ ويدير اثنين Materialized Views:
#
# 1) daily_sales_summary
#    ملخص المبيعات اليومية.
#
# 2) city_sales_summary
#    ملخص المبيعات حسب المدينة.
#
# الفكرة:
# - أول تشغيل يبني الـ Views من البيانات الحالية.
# - بعد ذلك لا نعيد بناء كل البيانات في كل Refresh.
# - نستخدم source_ingested_at كـ Watermark.
# - عند وجود بيانات جديدة أو تمت إعادة معالجتها،
#   نحدد الـ Buckets المتأثرة فقط ثم نعيد حسابها.
#
# مهم:
# source_ingested_at هو الحقل الصحيح للتحديث التدريجي
# لأنه يتغير عند مرور السجل عبر Phase 1 Ingestion.
# ============================================================

import argparse
import json

from datetime import (
    datetime,
    timedelta,
    timezone,
)

from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DB_NAME,
    VALIDATED_COLLECTION,
)


# ============================================================
# أسماء الـ Collections
# ============================================================

DAILY_VIEW = "daily_sales_summary"

CITY_VIEW = "city_sales_summary"

STATE_COLLECTION = "phase2_mv_state"


# ============================================================
# الحقل المستخدم كـ Watermark
# ============================================================

WATERMARK_FIELD = "source_ingested_at"


# ============================================================
# أسماء الـ Materialized Views
# ============================================================

VIEW_NAMES = [
    DAILY_VIEW,
    CITY_VIEW,
]


# ============================================================
# دوال مساعدة
# ============================================================

def utc_now():
    """
    إرجاع الوقت الحالي بصيغة UTC.

    يتم تخزينه بدون timezone حتى يتوافق
    مع datetime الموجود داخل MongoDB.
    """

    return (
        datetime
        .now(
            timezone.utc
        )
        .replace(
            tzinfo=None
        )
    )


def json_default(value):
    """
    تحويل القيم الخاصة مثل datetime إلى JSON.
    """

    if isinstance(
        value,
        datetime,
    ):

        return value.isoformat()

    return str(
        value
    )


def print_json(data):
    """
    طباعة JSON بشكل مرتب مع دعم اللغة العربية.
    """

    print(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            default=json_default,
        )
    )


def get_database():
    """
    الاتصال بقاعدة البيانات الرئيسية.
    """

    client = MongoClient(
        MONGO_URI
    )

    db = client[
        MONGO_DB_NAME
    ]

    return (
        client,
        db,
    )


def amount_expression():
    """
    تحويل total_amount إلى Double بطريقة آمنة.

    إذا كانت القيمة غير صالحة:
    يتم اعتبارها 0 بدلاً من إيقاف Aggregation.
    """

    return {
        "$convert": {
            "input": "$total_amount",
            "to": "double",
            "onError": 0,
            "onNull": 0,
        }
    }


# ============================================================
# Watermark
# ============================================================

def get_latest_watermark(
    source_collection,
):
    """
    معرفة أحدث source_ingested_at
    موجود في orders_validated.

    يعتمد على Index:

    idx_source_ingested_at_mv
    """

    document = (
        source_collection
        .find_one(
            {
                WATERMARK_FIELD: {
                    "$type": "date",
                }
            },
            {
                "_id": 0,
                WATERMARK_FIELD: 1,
            },
            sort=[
                (
                    WATERMARK_FIELD,
                    -1,
                )
            ],
        )
    )

    if not document:
        return None

    return document.get(
        WATERMARK_FIELD
    )


def get_view_state(
    db,
    view_name,
):
    """
    قراءة آخر Watermark خاص بالـ View.
    """

    return (
        db[
            STATE_COLLECTION
        ]
        .find_one(
            {
                "_id": view_name,
            }
        )
    )


def save_view_state(
    db,
    view_name,
    watermark,
    refresh_mode,
    affected_buckets,
):
    """
    حفظ حالة آخر Refresh.

    يتم أيضًا حفظ اسم الحقل المستخدم
    كـ Watermark للتوثيق.
    """

    db[
        STATE_COLLECTION
    ].update_one(
        {
            "_id": view_name,
        },
        {
            "$set": {
                "view_name": view_name,
                "watermark": watermark,
                "watermark_field": (
                    WATERMARK_FIELD
                ),
                "last_refresh_at": (
                    utc_now()
                ),
                "refresh_mode": (
                    refresh_mode
                ),
                "affected_buckets": (
                    affected_buckets
                ),
            }
        },
        upsert=True,
    )


# ============================================================
# Indexes الخاصة بالـ Materialized Views
# ============================================================

def ensure_mv_indexes(
    db,
):
    """
    إنشاء Indexes المطلوبة
    للـ Materialized Views.
    """

    # --------------------------------------------------------
    # Index مهم للـ Incremental Refresh
    # --------------------------------------------------------

    db[
        VALIDATED_COLLECTION
    ].create_index(
        [
            (
                WATERMARK_FIELD,
                1,
            )
        ],
        name=(
            "idx_source_ingested_at_mv"
        ),
    )

    # --------------------------------------------------------
    # Daily View Index
    # --------------------------------------------------------

    db[
        DAILY_VIEW
    ].create_index(
        [
            (
                "period",
                1,
            )
        ],
        name="idx_daily_period",
    )

    # --------------------------------------------------------
    # City View Index
    # --------------------------------------------------------

    db[
        CITY_VIEW
    ].create_index(
        [
            (
                "city",
                1,
            )
        ],
        name="idx_city_name",
    )

    # --------------------------------------------------------
    # State Collection Index
    # --------------------------------------------------------

    db[
        STATE_COLLECTION
    ].create_index(
        [
            (
                "last_refresh_at",
                -1,
            )
        ],
        name=(
            "idx_mv_last_refresh"
        ),
    )


# ============================================================
# Full Build - Daily Sales
# ============================================================

def build_daily_full(
    db,
    source_collection,
    cutoff_watermark,
):
    """
    بناء daily_sales_summary بالكامل.

    يستخدم فقط:
    - أول تشغيل.
    - أو عند طلب Rebuild يدوي.

    لا يستخدم في كل Refresh.
    """

    view_collection = db[
        DAILY_VIEW
    ]

    # --------------------------------------------------------
    # حذف النسخة القديمة عند Full Build فقط
    # --------------------------------------------------------

    view_collection.delete_many(
        {}
    )

    refreshed_at = utc_now()

    # --------------------------------------------------------
    # Aggregation Pipeline
    # --------------------------------------------------------

    pipeline = [
        {
            "$match": {
                "order_date": {
                    "$type": "string",
                }
            }
        },
        {
            "$project": {
                "period": {
                    "$substrBytes": [
                        "$order_date",
                        0,
                        10,
                    ]
                },
                "amount": (
                    amount_expression()
                ),
            }
        },
        {
            "$group": {
                "_id": "$period",
                "order_count": {
                    "$sum": 1,
                },
                "total_sales": {
                    "$sum": "$amount",
                },
            }
        },
        {
            "$set": {
                "period": "$_id",
                "average_order_value": {
                    "$round": [
                        {
                            "$cond": [
                                {
                                    "$gt": [
                                        "$order_count",
                                        0,
                                    ]
                                },
                                {
                                    "$divide": [
                                        "$total_sales",
                                        "$order_count",
                                    ]
                                },
                                0,
                            ]
                        },
                        2,
                    ]
                },
                "refreshed_at": (
                    refreshed_at
                ),
                "source_watermark": (
                    cutoff_watermark
                ),
                "watermark_field": (
                    WATERMARK_FIELD
                ),
            }
        },
        {
            "$merge": {
                "into": DAILY_VIEW,
                "on": "_id",
                "whenMatched": (
                    "replace"
                ),
                "whenNotMatched": (
                    "insert"
                ),
            }
        },
    ]

    # --------------------------------------------------------
    # استهلاك Cursor لضمان اكتمال $merge
    # --------------------------------------------------------

    for _ in source_collection.aggregate(
        pipeline,
        allowDiskUse=True,
    ):
        pass


# ============================================================
# Full Build - City Sales
# ============================================================

def build_city_full(
    db,
    source_collection,
    cutoff_watermark,
):
    """
    بناء city_sales_summary بالكامل.

    يستخدم فقط:
    - أول تشغيل.
    - أو عند طلب Rebuild يدوي.
    """

    view_collection = db[
        CITY_VIEW
    ]

    view_collection.delete_many(
        {}
    )

    refreshed_at = utc_now()

    # --------------------------------------------------------
    # Aggregation Pipeline
    # --------------------------------------------------------

    pipeline = [
        {
            "$project": {
                "city": {
                    "$ifNull": [
                        "$city",
                        "غير معروف",
                    ]
                },
                "amount": (
                    amount_expression()
                ),
            }
        },
        {
            "$group": {
                "_id": "$city",
                "order_count": {
                    "$sum": 1,
                },
                "total_sales": {
                    "$sum": "$amount",
                },
            }
        },
        {
            "$set": {
                "city": "$_id",
                "average_order_value": {
                    "$round": [
                        {
                            "$cond": [
                                {
                                    "$gt": [
                                        "$order_count",
                                        0,
                                    ]
                                },
                                {
                                    "$divide": [
                                        "$total_sales",
                                        "$order_count",
                                    ]
                                },
                                0,
                            ]
                        },
                        2,
                    ]
                },
                "refreshed_at": (
                    refreshed_at
                ),
                "source_watermark": (
                    cutoff_watermark
                ),
                "watermark_field": (
                    WATERMARK_FIELD
                ),
            }
        },
        {
            "$merge": {
                "into": CITY_VIEW,
                "on": "_id",
                "whenMatched": (
                    "replace"
                ),
                "whenNotMatched": (
                    "insert"
                ),
            }
        },
    ]

    for _ in source_collection.aggregate(
        pipeline,
        allowDiskUse=True,
    ):
        pass


# ============================================================
# Incremental Refresh - Daily
# ============================================================

def recompute_daily_bucket(
    source_collection,
    view_collection,
    period,
    cutoff_watermark,
):
    """
    إعادة حساب يوم واحد فقط.

    مثال:

    2025-04-30

    بدلاً من إعادة حساب جميع الأيام.
    """

    try:

        start_date = (
            datetime.strptime(
                period,
                "%Y-%m-%d",
            )
        )

    except ValueError:

        return False

    next_date = (
        start_date
        + timedelta(
            days=1
        )
    )

    start_value = (
        start_date.strftime(
            "%Y-%m-%dT00:00:00"
        )
    )

    end_value = (
        next_date.strftime(
            "%Y-%m-%dT00:00:00"
        )
    )

    # --------------------------------------------------------
    # إعادة حساب هذا اليوم من المصدر
    # --------------------------------------------------------

    pipeline = [
        {
            "$match": {
                "order_date": {
                    "$gte": (
                        start_value
                    ),
                    "$lt": (
                        end_value
                    ),
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "order_count": {
                    "$sum": 1,
                },
                "total_sales": {
                    "$sum": (
                        amount_expression()
                    ),
                },
            }
        },
    ]

    result = list(
        source_collection.aggregate(
            pipeline,
            allowDiskUse=True,
        )
    )

    # --------------------------------------------------------
    # إذا لم تعد هناك بيانات لهذا اليوم
    # --------------------------------------------------------

    if not result:

        view_collection.delete_one(
            {
                "_id": period,
            }
        )

        return True

    row = result[
        0
    ]

    order_count = row.get(
        "order_count",
        0,
    )

    total_sales = row.get(
        "total_sales",
        0,
    )

    average_order_value = 0

    if order_count:

        average_order_value = round(
            total_sales
            / order_count,
            2,
        )

    # --------------------------------------------------------
    # Replace / Upsert لهذا اليوم فقط
    # --------------------------------------------------------

    view_collection.replace_one(
        {
            "_id": period,
        },
        {
            "_id": period,
            "period": period,
            "order_count": (
                order_count
            ),
            "total_sales": (
                total_sales
            ),
            "average_order_value": (
                average_order_value
            ),
            "refreshed_at": (
                utc_now()
            ),
            "source_watermark": (
                cutoff_watermark
            ),
            "watermark_field": (
                WATERMARK_FIELD
            ),
        },
        upsert=True,
    )

    return True


def refresh_daily_incremental(
    db,
    source_collection,
    previous_watermark,
    cutoff_watermark,
):
    """
    Incremental Refresh للملخص اليومي.

    نبحث فقط عن السجلات التي تم إدخالها
    أو إعادة معالجتها بعد آخر Watermark
    باستخدام source_ingested_at.
    """

    view_collection = db[
        DAILY_VIEW
    ]

    # --------------------------------------------------------
    # فقط السجلات التي تغيرت بعد آخر Refresh
    # --------------------------------------------------------

    change_filter = {
        WATERMARK_FIELD: {
            "$gt": (
                previous_watermark
            ),
            "$lte": (
                cutoff_watermark
            ),
        }
    }

    affected_periods = set()

    cursor = (
        source_collection
        .find(
            change_filter,
            {
                "_id": 0,
                "order_date": 1,
            },
        )
    )

    # --------------------------------------------------------
    # تحديد الأيام المتأثرة فقط
    # --------------------------------------------------------

    for document in cursor:

        order_date = (
            document.get(
                "order_date"
            )
        )

        if not isinstance(
            order_date,
            str,
        ):
            continue

        if len(
            order_date
        ) < 10:
            continue

        period = (
            order_date[
                :10
            ]
        )

        try:

            datetime.strptime(
                period,
                "%Y-%m-%d",
            )

        except ValueError:

            continue

        affected_periods.add(
            period
        )

    # --------------------------------------------------------
    # إعادة حساب الأيام المتأثرة فقط
    # --------------------------------------------------------

    for period in sorted(
        affected_periods
    ):

        recompute_daily_bucket(
            source_collection,
            view_collection,
            period,
            cutoff_watermark,
        )

    return len(
        affected_periods
    )


# ============================================================
# Incremental Refresh - City
# ============================================================

def normalize_city(
    city,
):
    """
    معالجة المدينة الفارغة.
    """

    if city is None:

        return "غير معروف"

    return city


def recompute_city_bucket(
    source_collection,
    view_collection,
    source_city,
    cutoff_watermark,
):
    """
    إعادة حساب مدينة واحدة فقط.
    """

    city_name = (
        normalize_city(
            source_city
        )
    )

    # --------------------------------------------------------
    # فلتر المدينة
    # --------------------------------------------------------

    if source_city is None:

        match_filter = {
            "city": None,
        }

    else:

        match_filter = {
            "city": source_city,
        }

    pipeline = [
        {
            "$match": (
                match_filter
            ),
        },
        {
            "$group": {
                "_id": None,
                "order_count": {
                    "$sum": 1,
                },
                "total_sales": {
                    "$sum": (
                        amount_expression()
                    ),
                },
            }
        },
    ]

    result = list(
        source_collection.aggregate(
            pipeline,
            allowDiskUse=True,
        )
    )

    # --------------------------------------------------------
    # إذا لم تعد هناك بيانات لهذه المدينة
    # --------------------------------------------------------

    if not result:

        view_collection.delete_one(
            {
                "_id": city_name,
            }
        )

        return True

    row = result[
        0
    ]

    order_count = row.get(
        "order_count",
        0,
    )

    total_sales = row.get(
        "total_sales",
        0,
    )

    average_order_value = 0

    if order_count:

        average_order_value = round(
            total_sales
            / order_count,
            2,
        )

    # --------------------------------------------------------
    # تحديث Bucket المدينة
    # --------------------------------------------------------

    view_collection.replace_one(
        {
            "_id": city_name,
        },
        {
            "_id": city_name,
            "city": city_name,
            "order_count": (
                order_count
            ),
            "total_sales": (
                total_sales
            ),
            "average_order_value": (
                average_order_value
            ),
            "refreshed_at": (
                utc_now()
            ),
            "source_watermark": (
                cutoff_watermark
            ),
            "watermark_field": (
                WATERMARK_FIELD
            ),
        },
        upsert=True,
    )

    return True


def refresh_city_incremental(
    db,
    source_collection,
    previous_watermark,
    cutoff_watermark,
):
    """
    Incremental Refresh حسب المدن.

    يتم البحث باستخدام:

    source_ingested_at
    """

    view_collection = db[
        CITY_VIEW
    ]

    change_filter = {
        WATERMARK_FIELD: {
            "$gt": (
                previous_watermark
            ),
            "$lte": (
                cutoff_watermark
            ),
        }
    }

    affected_cities = set()

    cursor = (
        source_collection
        .find(
            change_filter,
            {
                "_id": 0,
                "city": 1,
            },
        )
    )

    # --------------------------------------------------------
    # معرفة المدن المتأثرة
    # --------------------------------------------------------

    for document in cursor:

        affected_cities.add(
            document.get(
                "city"
            )
        )

    # --------------------------------------------------------
    # إعادة حساب المدن المتأثرة فقط
    # --------------------------------------------------------

    for city in affected_cities:

        recompute_city_bucket(
            source_collection,
            view_collection,
            city,
            cutoff_watermark,
        )

    return len(
        affected_cities
    )


# ============================================================
# Refresh View
# ============================================================

def refresh_view(
    view_name,
    force_rebuild=False,
):
    """
    تشغيل Refresh لـ Materialized View واحد.

    إذا لم يوجد Watermark سابق:
        FULL_BUILD

    إذا وجد Watermark وكانت هناك تغييرات:
        INCREMENTAL

    إذا لم توجد تغييرات:
        NO_CHANGES
    """

    # --------------------------------------------------------
    # التحقق من اسم View
    # --------------------------------------------------------

    if view_name not in VIEW_NAMES:

        raise ValueError(
            (
                "Unknown Materialized View: "
                f"{view_name}"
            )
        )

    client, db = (
        get_database()
    )

    try:

        # ----------------------------------------------------
        # التأكد من Indexes
        # ----------------------------------------------------

        ensure_mv_indexes(
            db
        )

        source_collection = db[
            VALIDATED_COLLECTION
        ]

        # ----------------------------------------------------
        # أحدث source_ingested_at
        # ----------------------------------------------------

        cutoff_watermark = (
            get_latest_watermark(
                source_collection
            )
        )

        if cutoff_watermark is None:

            return {
                "view_name": view_name,
                "status": "NO_DATA",
                "message": (
                    "No validated data found."
                ),
                "watermark_field": (
                    WATERMARK_FIELD
                ),
            }

        # ----------------------------------------------------
        # قراءة State السابقة
        # ----------------------------------------------------

        state = get_view_state(
            db,
            view_name,
        )

        previous_watermark = None

        previous_watermark_field = None

        if state:

            previous_watermark = (
                state.get(
                    "watermark"
                )
            )

            previous_watermark_field = (
                state.get(
                    "watermark_field"
                )
            )

        # ----------------------------------------------------
        # Full Build
        # ----------------------------------------------------

        if (
            force_rebuild
            or previous_watermark is None
        ):

            if (
                view_name
                == DAILY_VIEW
            ):

                build_daily_full(
                    db,
                    source_collection,
                    cutoff_watermark,
                )

            elif (
                view_name
                == CITY_VIEW
            ):

                build_city_full(
                    db,
                    source_collection,
                    cutoff_watermark,
                )

            save_view_state(
                db,
                view_name,
                cutoff_watermark,
                "FULL_BUILD",
                "ALL",
            )

            return {
                "view_name": (
                    view_name
                ),
                "status": "SUCCESS",
                "refresh_mode": (
                    "FULL_BUILD"
                ),
                "watermark": (
                    cutoff_watermark
                ),
                "watermark_field": (
                    WATERMARK_FIELD
                ),
                "document_count": (
                    db[
                        view_name
                    ].count_documents(
                        {}
                    )
                ),
            }

        # ----------------------------------------------------
        # لا توجد تغييرات جديدة
        # ----------------------------------------------------

        if (
            cutoff_watermark
            <= previous_watermark
        ):

            # تحديث معلومات الـ State
            # لتوثيق اسم الحقل الجديد.
            save_view_state(
                db,
                view_name,
                previous_watermark,
                "NO_CHANGES",
                0,
            )

            return {
                "view_name": (
                    view_name
                ),
                "status": "SUCCESS",
                "refresh_mode": (
                    "NO_CHANGES"
                ),
                "watermark": (
                    previous_watermark
                ),
                "watermark_field": (
                    WATERMARK_FIELD
                ),
                "previous_watermark_field": (
                    previous_watermark_field
                ),
                "affected_buckets": 0,
                "document_count": (
                    db[
                        view_name
                    ].count_documents(
                        {}
                    )
                ),
            }

        # ----------------------------------------------------
        # Incremental Refresh
        # ----------------------------------------------------

        if (
            view_name
            == DAILY_VIEW
        ):

            affected_buckets = (
                refresh_daily_incremental(
                    db,
                    source_collection,
                    previous_watermark,
                    cutoff_watermark,
                )
            )

        else:

            affected_buckets = (
                refresh_city_incremental(
                    db,
                    source_collection,
                    previous_watermark,
                    cutoff_watermark,
                )
            )

        # ----------------------------------------------------
        # حفظ State الجديدة
        # ----------------------------------------------------

        save_view_state(
            db,
            view_name,
            cutoff_watermark,
            "INCREMENTAL",
            affected_buckets,
        )

        return {
            "view_name": (
                view_name
            ),
            "status": "SUCCESS",
            "refresh_mode": (
                "INCREMENTAL"
            ),
            "previous_watermark": (
                previous_watermark
            ),
            "new_watermark": (
                cutoff_watermark
            ),
            "watermark_field": (
                WATERMARK_FIELD
            ),
            "previous_watermark_field": (
                previous_watermark_field
            ),
            "affected_buckets": (
                affected_buckets
            ),
            "document_count": (
                db[
                    view_name
                ].count_documents(
                    {}
                )
            ),
        }

    finally:

        client.close()


# ============================================================
# Refresh All
# ============================================================

def refresh_all(
    force_rebuild=False,
):
    """
    تحديث جميع الـ Materialized Views.
    """

    results = []

    for view_name in VIEW_NAMES:

        result = refresh_view(
            view_name,
            force_rebuild=(
                force_rebuild
            ),
        )

        results.append(
            result
        )

    return {
        "status": "SUCCESS",
        "watermark_field": (
            WATERMARK_FIELD
        ),
        "views": results,
    }


# ============================================================
# List Views
# ============================================================

def list_views():
    """
    عرض الـ Materialized Views الموجودة في المشروع.
    """

    return {
        DAILY_VIEW: {
            "description": (
                "ملخص المبيعات اليومية المبني "
                "على sales_by_period."
            ),
            "source": (
                VALIDATED_COLLECTION
            ),
            "incremental_field": (
                WATERMARK_FIELD
            ),
        },

        CITY_VIEW: {
            "description": (
                "ملخص المبيعات حسب المدينة "
                "المبني على sales_by_city."
            ),
            "source": (
                VALIDATED_COLLECTION
            ),
            "incremental_field": (
                WATERMARK_FIELD
            ),
        },
    }


# ============================================================
# Status
# ============================================================

def get_status():
    """
    عرض حالة الـ Materialized Views.
    """

    client, db = (
        get_database()
    )

    try:

        results = []

        for view_name in VIEW_NAMES:

            state = get_view_state(
                db,
                view_name,
            )

            results.append(
                {
                    "view_name": (
                        view_name
                    ),
                    "document_count": (
                        db[
                            view_name
                        ].count_documents(
                            {}
                        )
                    ),
                    "watermark": (
                        state.get(
                            "watermark"
                        )
                        if state
                        else None
                    ),
                    "watermark_field": (
                        state.get(
                            "watermark_field"
                        )
                        if state
                        else None
                    ),
                    "last_refresh_at": (
                        state.get(
                            "last_refresh_at"
                        )
                        if state
                        else None
                    ),
                    "refresh_mode": (
                        state.get(
                            "refresh_mode"
                        )
                        if state
                        else None
                    ),
                    "affected_buckets": (
                        state.get(
                            "affected_buckets"
                        )
                        if state
                        else None
                    ),
                }
            )

        return {
            "database": (
                MONGO_DB_NAME
            ),
            "source_collection": (
                VALIDATED_COLLECTION
            ),
            "incremental_field": (
                WATERMARK_FIELD
            ),
            "views": results,
        }

    finally:

        client.close()


# ============================================================
# Command Line Interface
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Phase 2 Materialized Views Manager"
        )
    )

    # --------------------------------------------------------
    # List
    # --------------------------------------------------------

    parser.add_argument(
        "--list",
        action="store_true",
        help=(
            "List available "
            "Materialized Views."
        ),
    )

    # --------------------------------------------------------
    # Status
    # --------------------------------------------------------

    parser.add_argument(
        "--status",
        action="store_true",
        help=(
            "Show Materialized Views status."
        ),
    )

    # --------------------------------------------------------
    # Refresh One
    # --------------------------------------------------------

    parser.add_argument(
        "--refresh",
        choices=VIEW_NAMES,
        help=(
            "Refresh one Materialized View."
        ),
    )

    # --------------------------------------------------------
    # Refresh All
    # --------------------------------------------------------

    parser.add_argument(
        "--refresh-all",
        action="store_true",
        help=(
            "Refresh all Materialized Views."
        ),
    )

    # --------------------------------------------------------
    # Force Rebuild
    # --------------------------------------------------------

    parser.add_argument(
        "--rebuild",
        action="store_true",
        help=(
            "Force a full rebuild. "
            "Use only when needed."
        ),
    )

    args = parser.parse_args()

    # --------------------------------------------------------
    # List
    # --------------------------------------------------------

    if args.list:

        print_json(
            list_views()
        )

        return

    # --------------------------------------------------------
    # Status
    # --------------------------------------------------------

    if args.status:

        print_json(
            get_status()
        )

        return

    # --------------------------------------------------------
    # Refresh One
    # --------------------------------------------------------

    if args.refresh:

        result = refresh_view(
            args.refresh,
            force_rebuild=(
                args.rebuild
            ),
        )

        print_json(
            result
        )

        return

    # --------------------------------------------------------
    # Refresh All
    # --------------------------------------------------------

    if args.refresh_all:

        result = refresh_all(
            force_rebuild=(
                args.rebuild
            ),
        )

        print_json(
            result
        )

        return

    # --------------------------------------------------------
    # Help
    # --------------------------------------------------------

    parser.print_help()


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    main()