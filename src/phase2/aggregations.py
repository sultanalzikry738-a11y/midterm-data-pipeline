from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pymongo import MongoClient


# ============================================================
# Project paths
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    MONGO_URI,
    MONGO_DB_NAME,
    VALIDATED_COLLECTION,
)


REPORT_DIR = PROJECT_ROOT / "reports" / "phase2" / "aggregations"
REPORT_DIR.mkdir(
    parents=True,
    exist_ok=True,
)


# ============================================================
# Available Aggregation Reports
# ============================================================

AGGREGATION_DESCRIPTIONS = {

    "sales_by_city":
        "إجمالي المبيعات وعدد الطلبات ومتوسط قيمة الطلب حسب المدينة.",

    "top_customers":
        "أفضل العملاء حسب إجمالي قيمة مشترياتهم.",

    "sales_by_period":
        "إجمالي المبيعات وعدد الطلبات حسب اليوم.",

    "orders_by_status":
        "توزيع الطلبات حسب حالة الطلب مع إجمالي المبيعات.",

    "payment_status_summary":
        "توزيع الطلبات حسب حالة الدفع مع إجمالي المبالغ.",
}


# ============================================================
# MongoDB connection
# ============================================================

def get_collection():
    """
    الاتصال بقاعدة MongoDB
    وإرجاع orders_validated.
    """

    client = MongoClient(
        MONGO_URI
    )

    collection = client[
        MONGO_DB_NAME
    ][
        VALIDATED_COLLECTION
    ]

    return client, collection


# ============================================================
# Helpers
# ============================================================

def safe_limit(
    limit: int,
) -> int:
    """
    منع إرجاع عدد ضخم من النتائج.
    """

    return max(
        1,
        min(
            int(limit),
            100,
        ),
    )


def normalize_documents(
    documents: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    تحويل MongoDB values إلى قيم قابلة
    للعرض والحفظ في JSON.
    """

    normalized = []

    for document in documents:

        clean = {}

        for key, value in document.items():

            if isinstance(
                value,
                datetime,
            ):
                clean[key] = (
                    value.isoformat()
                )

            else:
                clean[key] = value

        normalized.append(
            clean
        )

    return normalized


# ============================================================
# Report 1
# Sales by City
# ============================================================

def sales_by_city(
    collection,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Aggregation 1:
    المبيعات حسب المدينة.
    """

    pipeline = [

        {
            "$match": {
                "city": {
                    "$exists": True,
                    "$nin": [
                        None,
                        "",
                    ],
                },
                "total_amount": {
                    "$type": "double",
                },
            }
        },

        {
            "$group": {
                "_id": "$city",

                "order_count": {
                    "$sum": 1,
                },

                "total_sales": {
                    "$sum":
                        "$total_amount",
                },

                "average_order_value": {
                    "$avg":
                        "$total_amount",
                },
            }
        },

        {
            "$sort": {
                "total_sales": -1,
            }
        },

        {
            "$limit":
                safe_limit(limit)
        },

        {
            "$project": {
                "_id": 0,

                "city": "$_id",

                "order_count": 1,

                "total_sales": {
                    "$round": [
                        "$total_sales",
                        2,
                    ]
                },

                "average_order_value": {
                    "$round": [
                        "$average_order_value",
                        2,
                    ]
                },
            }
        },
    ]

    return list(
        collection.aggregate(
            pipeline,
            allowDiskUse=True,
        )
    )


# ============================================================
# Report 2
# Top Customers
# ============================================================

def top_customers(
    collection,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Aggregation 2:
    أفضل العملاء حسب إجمالي مشترياتهم.
    """

    pipeline = [

        {
            "$match": {
                "customer_id": {
                    "$exists": True,
                    "$nin": [
                        None,
                        "",
                    ],
                },
                "total_amount": {
                    "$type": "double",
                },
            }
        },

        {
            "$group": {

                "_id": {
                    "customer_id":
                        "$customer_id",

                    "customer_name":
                        "$customer_name",
                },

                "order_count": {
                    "$sum": 1,
                },

                "total_spent": {
                    "$sum":
                        "$total_amount",
                },

                "average_order_value": {
                    "$avg":
                        "$total_amount",
                },
            }
        },

        {
            "$sort": {
                "total_spent": -1,
            }
        },

        {
            "$limit":
                safe_limit(limit)
        },

        {
            "$project": {

                "_id": 0,

                "customer_id":
                    "$_id.customer_id",

                "customer_name":
                    "$_id.customer_name",

                "order_count": 1,

                "total_spent": {
                    "$round": [
                        "$total_spent",
                        2,
                    ]
                },

                "average_order_value": {
                    "$round": [
                        "$average_order_value",
                        2,
                    ]
                },
            }
        },
    ]

    return list(
        collection.aggregate(
            pipeline,
            allowDiskUse=True,
        )
    )


# ============================================================
# Report 3
# Sales by Period
# ============================================================

def sales_by_period(
    collection,
    limit: int = 30,
) -> list[dict[str, Any]]:
    """
    Aggregation 3:
    المبيعات اليومية.

    order_date عندنا ISO String.
    نستخرج YYYY-MM-DD.
    """

    pipeline = [

        {
            "$match": {
                "order_date": {
                    "$exists": True,
                    "$nin": [
                        None,
                        "",
                    ],
                },

                "total_amount": {
                    "$type": "double",
                },
            }
        },

        {
            "$project": {

                "day": {
                    "$substrBytes": [
                        "$order_date",
                        0,
                        10,
                    ]
                },

                "total_amount": 1,
            }
        },

        {
            "$group": {

                "_id":
                    "$day",

                "order_count": {
                    "$sum": 1,
                },

                "total_sales": {
                    "$sum":
                        "$total_amount",
                },

                "average_order_value": {
                    "$avg":
                        "$total_amount",
                },
            }
        },

        {
            "$sort": {
                "_id": -1,
            }
        },

        {
            "$limit":
                safe_limit(limit)
        },

        {
            "$project": {

                "_id": 0,

                "period":
                    "$_id",

                "order_count": 1,

                "total_sales": {
                    "$round": [
                        "$total_sales",
                        2,
                    ]
                },

                "average_order_value": {
                    "$round": [
                        "$average_order_value",
                        2,
                    ]
                },
            }
        },
    ]

    return list(
        collection.aggregate(
            pipeline,
            allowDiskUse=True,
        )
    )


# ============================================================
# Report 4
# Orders by Status
# ============================================================

def orders_by_status(
    collection,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Aggregation 4:
    توزيع الطلبات حسب الحالة.
    """

    pipeline = [

        {
            "$match": {
                "status": {
                    "$exists": True,
                    "$nin": [
                        None,
                        "",
                    ],
                }
            }
        },

        {
            "$group": {

                "_id":
                    "$status",

                "order_count": {
                    "$sum": 1,
                },

                "total_sales": {
                    "$sum": {
                        "$ifNull": [
                            "$total_amount",
                            0,
                        ]
                    }
                },
            }
        },

        {
            "$sort": {
                "order_count": -1,
            }
        },

        {
            "$limit":
                safe_limit(limit)
        },

        {
            "$project": {

                "_id": 0,

                "status":
                    "$_id",

                "order_count": 1,

                "total_sales": {
                    "$round": [
                        "$total_sales",
                        2,
                    ]
                },
            }
        },
    ]

    return list(
        collection.aggregate(
            pipeline,
            allowDiskUse=True,
        )
    )


# ============================================================
# Report 5
# Payment Status Summary
# ============================================================

def payment_status_summary(
    collection,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Aggregation 5:
    توزيع الطلبات حسب حالة الدفع.
    """

    pipeline = [

        {
            "$match": {
                "payment_status": {
                    "$exists": True,
                    "$nin": [
                        None,
                        "",
                    ],
                }
            }
        },

        {
            "$group": {

                "_id":
                    "$payment_status",

                "order_count": {
                    "$sum": 1,
                },

                "total_order_value": {
                    "$sum": {
                        "$ifNull": [
                            "$total_amount",
                            0,
                        ]
                    }
                },

                "total_payment_amount": {
                    "$sum": {
                        "$ifNull": [
                            "$payment_amount",
                            0,
                        ]
                    }
                },
            }
        },

        {
            "$sort": {
                "order_count": -1,
            }
        },

        {
            "$limit":
                safe_limit(limit)
        },

        {
            "$project": {

                "_id": 0,

                "payment_status":
                    "$_id",

                "order_count": 1,

                "total_order_value": {
                    "$round": [
                        "$total_order_value",
                        2,
                    ]
                },

                "total_payment_amount": {
                    "$round": [
                        "$total_payment_amount",
                        2,
                    ]
                },
            }
        },
    ]

    return list(
        collection.aggregate(
            pipeline,
            allowDiskUse=True,
        )
    )


# ============================================================
# Public functions
# ============================================================

def get_aggregation_names() -> list[str]:
    """
    سترجعها FastAPI لاحقًا.
    """

    return list(
        AGGREGATION_DESCRIPTIONS.keys()
    )


def run_aggregation(
    name: str,
    limit: int = 20,
) -> dict[str, Any]:
    """
    تشغيل Aggregation Report بالاسم.

    هذه الدالة ستستخدمها FastAPI أيضًا.
    """

    client, collection = (
        get_collection()
    )

    try:

        if name == "sales_by_city":

            rows = sales_by_city(
                collection,
                limit,
            )

        elif name == "top_customers":

            rows = top_customers(
                collection,
                limit,
            )

        elif name == "sales_by_period":

            rows = sales_by_period(
                collection,
                limit,
            )

        elif name == "orders_by_status":

            rows = orders_by_status(
                collection,
                limit,
            )

        elif name == "payment_status_summary":

            rows = payment_status_summary(
                collection,
                limit,
            )

        else:

            raise ValueError(
                f"Unknown aggregation: {name}"
            )

        rows = normalize_documents(
            rows
        )

        return {

            "aggregation_name":
                name,

            "description":
                AGGREGATION_DESCRIPTIONS[
                    name
                ],

            "database":
                MONGO_DB_NAME,

            "collection":
                VALIDATED_COLLECTION,

            "generated_at":
                datetime.now(
                    timezone.utc
                ).isoformat(),

            "returned_count":
                len(rows),

            "results":
                rows,
        }

    finally:

        client.close()


# ============================================================
# Save report
# ============================================================

def save_report(
    name: str,
    result: dict[str, Any],
) -> Path:
    """
    حفظ نتيجة التقرير كدليل داخل reports/phase2.
    """

    output_file = (
        REPORT_DIR
        / f"{name}.json"
    )

    output_file.write_text(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    return output_file


# ============================================================
# CLI
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Phase 2 - MongoDB "
            "Aggregation Reports"
        )
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help=(
            "List available reports."
        ),
    )

    parser.add_argument(
        "--name",
        help=(
            "Aggregation report name."
        ),
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help=(
            "Maximum returned groups."
        ),
    )

    parser.add_argument(
        "--save",
        action="store_true",
        help=(
            "Save report to reports/phase2."
        ),
    )

    args = parser.parse_args()


    # ----------------------------------------
    # List reports
    # ----------------------------------------

    if args.list:

        print(
            json.dumps(
                AGGREGATION_DESCRIPTIONS,
                ensure_ascii=False,
                indent=2,
            )
        )

        return


    # ----------------------------------------
    # Run one report
    # ----------------------------------------

    if not args.name:

        parser.error(
            "Use --list or provide --name."
        )


    result = run_aggregation(
        args.name,
        args.limit,
    )


    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


    # ----------------------------------------
    # Save result
    # ----------------------------------------

    if args.save:

        output_file = save_report(
            args.name,
            result,
        )

        print(
            "\nSaved:",
            output_file,
        )


if __name__ == "__main__":
    main()