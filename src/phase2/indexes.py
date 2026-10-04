from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pymongo import MongoClient, ASCENDING, DESCENDING


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


REPORT_DIR = PROJECT_ROOT / "reports" / "phase2"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# Phase 2 indexes
# ============================================================

# هذه الفهارس تخدم الاستعلامات الخمسة الموجودة في queries.py
INDEX_DEFINITIONS = {

    # Query 1:
    # orders_by_date_range
    "idx_order_date": [
        ("order_date", ASCENDING),
    ],

    # Query 2:
    # customer_order_history
    # Compound Index
    "idx_customer_date": [
        ("customer_id", ASCENDING),
        ("order_date", DESCENDING),
    ],

    # Query 3:
    # orders_by_city_status
    # Compound Index
    "idx_city_status_date": [
        ("city", ASCENDING),
        ("status", ASCENDING),
        ("order_date", DESCENDING),
    ],

    # Query 4:
    # high_value_orders
    "idx_total_amount": [
        ("total_amount", DESCENDING),
    ],

    # Query 5:
    # orders_by_payment_status
    # Compound Index
    "idx_payment_status_date": [
        ("payment_status", ASCENDING),
        ("order_date", DESCENDING),
    ],
}


# سبب اختيار كل Index
INDEX_REASONS = {

    "idx_order_date":
        "يسرع البحث عن الطلبات ضمن فترة زمنية باستخدام order_date.",

    "idx_customer_date":
        "Compound Index لتسريع سجل طلبات العميل مع دعم ترتيب النتائج حسب التاريخ.",

    "idx_city_status_date":
        "Compound Index لتسريع البحث حسب المدينة والحالة مع دعم ترتيب الطلبات حسب التاريخ.",

    "idx_total_amount":
        "يسرع البحث عن الطلبات مرتفعة القيمة ويدعم ترتيبها حسب total_amount.",

    "idx_payment_status_date":
        "Compound Index لتسريع البحث حسب حالة الدفع مع دعم ترتيب النتائج حسب التاريخ.",
}


# ============================================================
# MongoDB connection
# ============================================================

def get_collection():
    """الاتصال بقاعدة MongoDB وإرجاع Collection الطلبات المعتمدة."""

    client = MongoClient(MONGO_URI)

    db = client[MONGO_DB_NAME]

    collection = db[VALIDATED_COLLECTION]

    return client, db, collection


# ============================================================
# Dynamic test values
# ============================================================

def get_dynamic_parameters(collection) -> dict[str, Any]:
    """
    اختيار قيم حقيقية من قاعدة البيانات الحالية.

    مهم:
    لا نعتمد على أسماء عملاء أو مدن أو نتائج ثابتة داخل الكود،
    لأن بيانات الاختبار عند الدكتور قد تكون مختلفة.
    """

    sample = collection.find_one(
        {
            "order_date": {"$exists": True},
            "customer_id": {"$exists": True},
            "city": {"$exists": True},
            "status": {"$exists": True},
        },
        {
            "_id": 0,
            "order_date": 1,
            "customer_id": 1,
            "city": 1,
            "status": 1,
        },
    )

    if not sample:
        raise RuntimeError(
            "No suitable document found in orders_validated."
        )

    raw_date = str(sample["order_date"])

    # order_date مخزن بصيغة ISO String
    day = raw_date[:10]

    return {
        "start_date": f"{day}T00:00:00",
        "end_date": f"{day}T23:59:59",
        "customer_id": sample["customer_id"],
        "city": sample["city"],
        "status": sample["status"],
    }


# ============================================================
# Explain helpers
# ============================================================

def collect_stages(node: Any) -> list[str]:
    """
    استخراج مراحل خطة MongoDB.

    أمثلة:
    COLLSCAN
    IXSCAN
    FETCH
    LIMIT
    """

    stages: list[str] = []

    if isinstance(node, dict):

        stage = node.get("stage")

        if stage:
            stages.append(str(stage))

        for value in node.values():
            stages.extend(
                collect_stages(value)
            )

    elif isinstance(node, list):

        for item in node:
            stages.extend(
                collect_stages(item)
            )

    return stages


def run_explain(
    db,
    collection,
    query_name: str,
    filter_doc: dict[str, Any],
) -> dict[str, Any]:
    """
    تشغيل explain("executionStats") على Query واحدة.
    """

    command = {
        "find": collection.name,
        "filter": filter_doc,
        "limit": 20,
    }

    explain = db.command(
        "explain",
        command,
        verbosity="executionStats",
    )

    execution = explain.get(
        "executionStats",
        {},
    )

    query_planner = explain.get(
        "queryPlanner",
        {},
    )

    winning_plan = query_planner.get(
        "winningPlan",
        {},
    )

    stages = sorted(
        set(
            collect_stages(
                winning_plan
            )
        )
    )

    return {
        "query_name": query_name,

        "filter": filter_doc,

        "n_returned":
            execution.get(
                "nReturned"
            ),

        "execution_time_ms":
            execution.get(
                "executionTimeMillis"
            ),

        "total_docs_examined":
            execution.get(
                "totalDocsExamined"
            ),

        "total_keys_examined":
            execution.get(
                "totalKeysExamined"
            ),

        "plan_stages":
            stages,

        "uses_collscan":
            "COLLSCAN" in stages,

        "uses_ixscan":
            "IXSCAN" in stages,
    }


# ============================================================
# Capture Explain
# ============================================================

def capture_explain(
    label: str,
) -> dict[str, Any]:
    """
    حفظ Explain لثلاثة Queries رئيسية.

    الدكتور يطلب على الأقل 3 Queries
    قبل وبعد إنشاء Indexes.
    """

    client, db, collection = get_collection()

    try:

        params = get_dynamic_parameters(
            collection
        )

        explain_queries = [

            # ------------------------------------------------
            # Query 1
            # ------------------------------------------------
            (
                "orders_by_date_range",
                {
                    "order_date": {
                        "$gte":
                            params["start_date"],

                        "$lte":
                            params["end_date"],
                    }
                },
            ),

            # ------------------------------------------------
            # Query 2
            # ------------------------------------------------
            (
                "customer_order_history",
                {
                    "customer_id":
                        params["customer_id"]
                },
            ),

            # ------------------------------------------------
            # Query 3
            # ------------------------------------------------
            (
                "orders_by_city_status",
                {
                    "city":
                        params["city"],

                    "status":
                        params["status"],
                },
            ),
        ]

        results = []

        for (
            query_name,
            filter_doc,
        ) in explain_queries:

            print(
                f"\nRunning Explain: "
                f"{query_name}"
            )

            result = run_explain(
                db,
                collection,
                query_name,
                filter_doc,
            )

            results.append(
                result
            )

            print(
                f"  nReturned          = "
                f"{result['n_returned']}"
            )

            print(
                f"  Docs Examined      = "
                f"{result['total_docs_examined']}"
            )

            print(
                f"  Keys Examined      = "
                f"{result['total_keys_examined']}"
            )

            print(
                f"  Execution Time ms  = "
                f"{result['execution_time_ms']}"
            )

            print(
                f"  Plan Stages        = "
                f"{result['plan_stages']}"
            )

        output = {

            "phase":
                label,

            "captured_at":
                datetime.now(
                    timezone.utc
                ).isoformat(),

            "database":
                MONGO_DB_NAME,

            "collection":
                VALIDATED_COLLECTION,

            "collection_count":
                collection
                .estimated_document_count(),

            "dynamic_parameters":
                params,

            "indexes_at_capture":
                collection
                .index_information(),

            "results":
                results,
        }

        output_file = (
            REPORT_DIR
            / f"explain_{label}.json"
        )

        output_file.write_text(
            json.dumps(
                output,
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )

        print(
            "\nSaved:",
            output_file,
        )

        return output

    finally:
        client.close()


# ============================================================
# Create indexes
# ============================================================

def create_phase2_indexes() -> dict[str, Any]:
    """
    إنشاء جميع Indexes الخاصة بـPhase 2.

    create_index آمنة لإعادة التشغيل:
    إذا كان نفس Index موجودًا بنفس الاسم والتعريف
    فلن ينشئ نسخة مكررة.
    """

    client, _, collection = get_collection()

    try:

        created = {}

        print(
            "\nCreating Phase 2 indexes..."
        )

        for (
            name,
            keys,
        ) in INDEX_DEFINITIONS.items():

            print(
                f"\nCreating: {name}"
            )

            print(
                f"Reason   : "
                f"{INDEX_REASONS[name]}"
            )

            actual_name = (
                collection.create_index(
                    keys,
                    name=name,
                )
            )

            created[name] = {

                "actual_name":
                    actual_name,

                "keys":
                    keys,

                "reason":
                    INDEX_REASONS[name],
            }

        print(
            "\nPHASE 2 INDEX CREATION: PASS"
        )

        return created

    finally:
        client.close()


# ============================================================
# List indexes
# ============================================================

def list_indexes():
    """عرض جميع Indexes الموجودة حاليًا."""

    client, _, collection = get_collection()

    try:

        print(
            json.dumps(
                collection
                .index_information(),

                ensure_ascii=False,

                indent=2,

                default=str,
            )
        )

    finally:
        client.close()


# ============================================================
# CLI
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Phase 2 - MongoDB Indexes "
            "and Explain executionStats"
        )
    )

    parser.add_argument(
        "--before",
        action="store_true",
        help=(
            "Capture Explain BEFORE indexes."
        ),
    )

    parser.add_argument(
        "--create",
        action="store_true",
        help=(
            "Create Phase 2 indexes."
        ),
    )

    parser.add_argument(
        "--after",
        action="store_true",
        help=(
            "Capture Explain AFTER indexes."
        ),
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help=(
            "List MongoDB indexes."
        ),
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help=(
            "Run BEFORE -> CREATE -> AFTER."
        ),
    )

    args = parser.parse_args()

    # عرض الفهارس فقط
    if args.list:

        list_indexes()

        return

    # تنفيذ جميع الخطوات
    if args.all:

        capture_explain(
            "before"
        )

        create_phase2_indexes()

        capture_explain(
            "after"
        )

        return

    # Explain قبل الفهارس
    if args.before:

        capture_explain(
            "before"
        )

    # إنشاء الفهارس
    if args.create:

        create_phase2_indexes()

    # Explain بعد الفهارس
    if args.after:

        capture_explain(
            "after"
        )

    # إذا لم يحدد المستخدم أمرًا
    if not any(
        [
            args.before,
            args.create,
            args.after,
            args.list,
            args.all,
        ]
    ):

        parser.print_help()


if __name__ == "__main__":
    main()