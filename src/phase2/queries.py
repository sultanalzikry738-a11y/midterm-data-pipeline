from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from pymongo import MongoClient, DESCENDING

# إضافة جذر المشروع للمسار حتى يعمل الملف من أي مكان
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import MONGO_URI, MONGO_DB_NAME, VALIDATED_COLLECTION


# أسماء ووصف الاستعلامات المتاحة
QUERY_DESCRIPTIONS = {
    "orders_by_date_range": "الطلبات الواقعة بين تاريخ بداية وتاريخ نهاية.",
    "customer_order_history": "سجل طلبات عميل محدد مرتبًا من الأحدث إلى الأقدم.",
    "orders_by_city_status": "الطلبات حسب المدينة وحالة الطلب.",
    "high_value_orders": "الطلبات التي إجماليها يساوي أو يتجاوز مبلغًا محددًا.",
    "orders_by_payment_status": "الطلبات حسب حالة الدفع.",
}


def get_collection():
    """إرجاع Collection الطلبات المعتمدة."""
    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB_NAME][VALIDATED_COLLECTION]
    return client, collection


def _safe_limit(limit: int) -> int:
    """منع إرجاع عدد ضخم من السجلات أثناء الاختبار."""
    return max(1, min(int(limit), 100))


def orders_by_date_range(
    collection,
    start_date: str,
    end_date: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Query 1: الطلبات خلال فترة زمنية."""
    query = {
        "order_date": {
            "$gte": start_date,
            "$lte": end_date,
        }
    }

    projection = {
        "_id": 0,
        "order_id": 1,
        "order_date": 1,
        "customer_id": 1,
        "city": 1,
        "status": 1,
        "total_amount": 1,
    }

    return list(
        collection.find(query, projection)
        .sort("order_date", DESCENDING)
        .limit(_safe_limit(limit))
    )


def customer_order_history(
    collection,
    customer_id: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Query 2: تاريخ طلبات عميل محدد."""
    query = {"customer_id": customer_id}

    projection = {
        "_id": 0,
        "order_id": 1,
        "order_date": 1,
        "customer_id": 1,
        "customer_name": 1,
        "status": 1,
        "payment_status": 1,
        "total_amount": 1,
    }

    return list(
        collection.find(query, projection)
        .sort("order_date", DESCENDING)
        .limit(_safe_limit(limit))
    )


def orders_by_city_status(
    collection,
    city: str,
    status: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Query 3: الطلبات حسب المدينة والحالة."""
    query = {
        "city": city,
        "status": status,
    }

    projection = {
        "_id": 0,
        "order_id": 1,
        "order_date": 1,
        "city": 1,
        "district": 1,
        "status": 1,
        "customer_id": 1,
        "total_amount": 1,
    }

    return list(
        collection.find(query, projection)
        .sort("order_date", DESCENDING)
        .limit(_safe_limit(limit))
    )


def high_value_orders(
    collection,
    min_amount: float,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Query 4: الطلبات ذات القيمة المرتفعة."""
    query = {
        "total_amount": {
            "$gte": float(min_amount),
        }
    }

    projection = {
        "_id": 0,
        "order_id": 1,
        "order_date": 1,
        "customer_id": 1,
        "city": 1,
        "total_amount": 1,
        "currency": 1,
    }

    return list(
        collection.find(query, projection)
        .sort("total_amount", DESCENDING)
        .limit(_safe_limit(limit))
    )


def orders_by_payment_status(
    collection,
    payment_status: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Query 5: الطلبات حسب حالة الدفع."""
    query = {"payment_status": payment_status}

    projection = {
        "_id": 0,
        "order_id": 1,
        "order_date": 1,
        "customer_id": 1,
        "payment_method": 1,
        "payment_status": 1,
        "total_amount": 1,
    }

    return list(
        collection.find(query, projection)
        .sort("order_date", DESCENDING)
        .limit(_safe_limit(limit))
    )


def get_query_names() -> list[str]:
    """إرجاع أسماء جميع الاستعلامات المتاحة."""
    return list(QUERY_DESCRIPTIONS.keys())


def run_query(
    name: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    """تشغيل Query بالاسم، وستستخدمها الـAPI لاحقًا."""
    client, collection = get_collection()

    try:
        limit = int(params.get("limit", 20))

        if name == "orders_by_date_range":
            rows = orders_by_date_range(
                collection,
                start_date=str(params["start_date"]),
                end_date=str(params["end_date"]),
                limit=limit,
            )

        elif name == "customer_order_history":
            rows = customer_order_history(
                collection,
                customer_id=str(params["customer_id"]),
                limit=limit,
            )

        elif name == "orders_by_city_status":
            rows = orders_by_city_status(
                collection,
                city=str(params["city"]),
                status=str(params["status"]),
                limit=limit,
            )

        elif name == "high_value_orders":
            rows = high_value_orders(
                collection,
                min_amount=float(params["min_amount"]),
                limit=limit,
            )

        elif name == "orders_by_payment_status":
            rows = orders_by_payment_status(
                collection,
                payment_status=str(params["payment_status"]),
                limit=limit,
            )

        else:
            raise ValueError(f"Unknown query: {name}")

        return {
            "query_name": name,
            "description": QUERY_DESCRIPTIONS[name],
            "returned_count": len(rows),
            "results": rows,
        }

    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(
        description="Phase 2 - Practical MongoDB Queries"
    )

    parser.add_argument("--list", action="store_true")
    parser.add_argument("--name")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--customer-id")
    parser.add_argument("--city")
    parser.add_argument("--status")
    parser.add_argument("--min-amount", type=float)
    parser.add_argument("--payment-status")
    parser.add_argument("--limit", type=int, default=20)

    args = parser.parse_args()

    if args.list:
        print(
            json.dumps(
                QUERY_DESCRIPTIONS,
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if not args.name:
        parser.error("Use --list or provide --name.")

    params = {
        "start_date": args.start_date,
        "end_date": args.end_date,
        "customer_id": args.customer_id,
        "city": args.city,
        "status": args.status,
        "min_amount": args.min_amount,
        "payment_status": args.payment_status,
        "limit": args.limit,
    }

    # إزالة القيم غير المستخدمة
    params = {
        key: value
        for key, value in params.items()
        if value is not None
    }

    try:
        result = run_query(args.name, params)
    except KeyError as exc:
        parser.error(f"Missing required parameter: {exc.args[0]}")
        return

    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
