# ============================================================
# Phase 2 - Unified FastAPI
# ============================================================
#
# هذا الملف يمثل طبقة API موحدة فوق المشروع الحالي.
#
# مهم:
# - لا نعيد بناء Phase 1 داخل FastAPI.
# - POST /ingest يستدعي run_pipeline() الموجود أصلًا.
# - Queries تستدعي queries.py.
# - Indexes تستدعي indexes.py.
# - Aggregations تستدعي aggregations.py.
# - Materialized Views تستدعي materialized_views.py.
# - Jobs تستدعي jobs.py.
#
# Swagger UI:
# http://127.0.0.1:8000/docs
# ============================================================

import json
from pathlib import Path
from typing import Any

from fastapi import (
    FastAPI,
    HTTPException,
)

from fastapi.encoders import jsonable_encoder

from pydantic import (
    BaseModel,
    Field,
)

from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DB_NAME,
)


# ============================================================
# Phase 1
# ============================================================

from src.main import run_pipeline


# ============================================================
# Phase 2 - Queries
# ============================================================

from src.phase2.queries import (
    get_query_names,
    run_query,
)


# ============================================================
# Phase 2 - Indexes
# ============================================================

from src.phase2.indexes import (
    create_phase2_indexes,
    list_indexes,
)


# ============================================================
# Phase 2 - Aggregations
# ============================================================

from src.phase2.aggregations import (
    get_aggregation_names,
    run_aggregation,
)


# ============================================================
# Phase 2 - Materialized Views
# ============================================================

from src.phase2.materialized_views import (
    list_views,
    refresh_all,
    refresh_view,
)


# ============================================================
# Phase 2 - Jobs
# ============================================================

from src.phase2.jobs import (
    get_jobs,
    run_job,
)


# ============================================================
# Project Path
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]


# ============================================================
# FastAPI Application
# ============================================================

app = FastAPI(
    title="Hybrid Big Data ELT Pipeline API",
    description=(
        "Unified FastAPI wrapper for Phase 1 and Phase 2 "
        "of the Hybrid Big Data ELT Pipeline."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)


# ============================================================
# Request Models
# ============================================================

class IngestRequest(BaseModel):
    """
    البيانات المطلوبة لتشغيل Phase 1 Ingestion.
    """

    input_path: str = Field(
        ...,
        description=(
            "Absolute or project-relative path "
            "to the CSV input file."
        ),
        examples=[
            "data/orders_small_sample.csv"
        ],
    )


class RefreshMVRequest(BaseModel):
    """
    خيارات تحديث Materialized Views.
    """

    view_name: str | None = Field(
        default=None,
        description=(
            "Optional Materialized View name. "
            "If omitted, all views are refreshed."
        ),
    )

    force_rebuild: bool = Field(
        default=False,
        description=(
            "Force FULL_BUILD. "
            "Normally keep this false so incremental "
            "refresh is used."
        ),
    )


# ============================================================
# JSON Helper
# ============================================================

def make_json_safe(
    value: Any,
):
    """
    تحويل النتيجة إلى شكل آمن لإرجاعه عبر JSON.

    يدعم:
    - datetime
    - Path
    - MongoDB ObjectId
    - أي قيم أخرى غير قابلة للتحويل مباشرة
    """

    try:
        return jsonable_encoder(
            value
        )

    except Exception:
        return json.loads(
            json.dumps(
                value,
                default=str,
                ensure_ascii=False,
            )
        )


# ============================================================
# Root
# ============================================================

@app.get(
    "/",
    tags=["System"],
)
def root():
    """
    الصفحة الأساسية للـ API.
    """

    return {
        "project": (
            "Hybrid Big Data ELT Pipeline"
        ),
        "phase": "Phase 2",
        "api": "FastAPI",
        "status": "RUNNING",
        "swagger": "/docs",
    }


# ============================================================
# GET /health
# ============================================================

@app.get(
    "/health",
    tags=["System"],
)
def health():
    """
    فحص:
    - FastAPI
    - MongoDB
    """

    client = None

    try:
        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=3000,
        )

        client.admin.command(
            "ping"
        )

        return {
            "status": "HEALTHY",
            "api": "UP",
            "mongodb": "UP",
            "database": MONGO_DB_NAME,
        }

    except Exception as error:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "UNHEALTHY",
                "api": "UP",
                "mongodb": "DOWN",
                "error": str(
                    error
                ),
            },
        )

    finally:
        if client is not None:
            client.close()


# ============================================================
# POST /ingest
# ============================================================

@app.post(
    "/ingest",
    tags=["Phase 1 - Ingestion"],
)
def ingest(
    request: IngestRequest,
):
    """
    تشغيل نفس Phase 1 Pipeline الموجودة أصلًا.

    لا يوجد Ingestion Path جديد هنا.

    FastAPI فقط يستدعي:

    run_pipeline(
        input_path=...,
        execute=True
    )
    """

    try:
        input_path = Path(
            request.input_path
        ).expanduser()

        # ----------------------------------------------------
        # إذا كان المسار Relative
        # نعتبره نسبة إلى مجلد المشروع.
        # ----------------------------------------------------

        if not input_path.is_absolute():

            input_path = (
                PROJECT_ROOT
                / input_path
            )

        input_path = (
            input_path.resolve()
        )

        # ----------------------------------------------------
        # التحقق من الملف
        # ----------------------------------------------------

        if not input_path.exists():

            raise HTTPException(
                status_code=404,
                detail=(
                    f"Input file not found: "
                    f"{input_path}"
                ),
            )

        if not input_path.is_file():

            raise HTTPException(
                status_code=400,
                detail=(
                    f"Input path is not a file: "
                    f"{input_path}"
                ),
            )

        # ----------------------------------------------------
        # نفس Phase 1 Pipeline
        # ----------------------------------------------------

        result = run_pipeline(
            input_path=input_path,
            execute=True,
        )

        return {
            "status": "SUCCESS",
            "input_path": str(
                input_path
            ),
            "pipeline_result": make_json_safe(
                result
            ),
        }

    except HTTPException:
        raise

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail={
                "status": "FAILED",
                "error": str(
                    error
                ),
            },
        )


# ============================================================
# POST /indexes
# ============================================================

@app.post(
    "/indexes",
    tags=["Phase 2 - Indexes"],
)
def create_indexes():
    """
    إنشاء Indexes الخاصة بـ Phase 2.

    العملية Idempotent.
    """

    try:
        result = (
            create_phase2_indexes()
        )

        return {
            "status": "SUCCESS",
            "result": make_json_safe(
                result
            ),
        }

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=str(
                error
            ),
        )


# ============================================================
# GET /indexes
# ============================================================

@app.get(
    "/indexes",
    tags=["Phase 2 - Indexes"],
)
def get_indexes():
    """
    عرض Indexes الحالية.
    """

    try:
        result = list_indexes()

        return {
            "status": "SUCCESS",
            "indexes": make_json_safe(
                result
            ),
        }

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=str(
                error
            ),
        )


# ============================================================
# GET /queries
# ============================================================

@app.get(
    "/queries",
    tags=["Phase 2 - Queries"],
)
def queries():
    """
    عرض أسماء جميع Queries المتاحة.
    """

    try:
        names = get_query_names()

        return {
            "count": len(
                names
            ),
            "queries": names,
        }

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=str(
                error
            ),
        )


# ============================================================
# GET /queries/{name}
# ============================================================

@app.get(
    "/queries/{name}",
    tags=["Phase 2 - Queries"],
)
def execute_query(
    name: str,

    start_date: str | None = None,

    end_date: str | None = None,

    customer_id: str | None = None,

    city: str | None = None,

    status: str | None = None,

    min_amount: float | None = None,

    payment_status: str | None = None,

    limit: int = 20,
):
    """
    تشغيل Query بالاسم.

    جميع Parameters تظهر مباشرة داخل Swagger.

    Queries المتاحة:

    1. orders_by_date_range

    2. customer_order_history

    3. orders_by_city_status

    4. high_value_orders

    5. orders_by_payment_status
    """

    available_queries = (
        get_query_names()
    )

    # --------------------------------------------------------
    # التحقق من اسم Query
    # --------------------------------------------------------

    if name not in available_queries:

        raise HTTPException(
            status_code=404,
            detail={
                "message": (
                    f"Unknown query: {name}"
                ),
                "available_queries": (
                    available_queries
                ),
            },
        )

    # --------------------------------------------------------
    # التحقق من Limit
    # --------------------------------------------------------

    if limit < 1:

        raise HTTPException(
            status_code=400,
            detail=(
                "limit must be greater than 0."
            ),
        )

    try:

        # ====================================================
        # 1. Orders By Date Range
        # ====================================================

        if name == "orders_by_date_range":

            if (
                not start_date
                or not end_date
            ):

                raise HTTPException(
                    status_code=400,
                    detail=(
                        "start_date and end_date "
                        "are required for "
                        "orders_by_date_range."
                    ),
                )

            params = {
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
            }

        # ====================================================
        # 2. Customer Order History
        # ====================================================

        elif name == "customer_order_history":

            if not customer_id:

                raise HTTPException(
                    status_code=400,
                    detail=(
                        "customer_id is required "
                        "for customer_order_history."
                    ),
                )

            params = {
                "customer_id": customer_id,
                "limit": limit,
            }

        # ====================================================
        # 3. Orders By City + Status
        # ====================================================

        elif name == "orders_by_city_status":

            if (
                not city
                or not status
            ):

                raise HTTPException(
                    status_code=400,
                    detail=(
                        "city and status are required "
                        "for orders_by_city_status."
                    ),
                )

            params = {
                "city": city,
                "status": status,
                "limit": limit,
            }

        # ====================================================
        # 4. High Value Orders
        # ====================================================

        elif name == "high_value_orders":

            if min_amount is None:

                raise HTTPException(
                    status_code=400,
                    detail=(
                        "min_amount is required "
                        "for high_value_orders."
                    ),
                )

            params = {
                "min_amount": min_amount,
                "limit": limit,
            }

        # ====================================================
        # 5. Orders By Payment Status
        # ====================================================

        elif name == "orders_by_payment_status":

            if not payment_status:

                raise HTTPException(
                    status_code=400,
                    detail=(
                        "payment_status is required "
                        "for orders_by_payment_status."
                    ),
                )

            params = {
                "payment_status": (
                    payment_status
                ),
                "limit": limit,
            }

        else:

            raise HTTPException(
                status_code=404,
                detail=(
                    f"Unknown query: {name}"
                ),
            )

        # ----------------------------------------------------
        # تشغيل Query الأصلية
        # ----------------------------------------------------

        result = run_query(
            name=name,
            params=params,
        )

        return make_json_safe(
            result
        )

    except HTTPException:
        raise

    except (
        TypeError,
        ValueError,
        KeyError,
    ) as error:

        raise HTTPException(
            status_code=400,
            detail=str(
                error
            ),
        )

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=str(
                error
            ),
        )


# ============================================================
# GET /aggregations
# ============================================================

@app.get(
    "/aggregations",
    tags=["Phase 2 - Aggregations"],
)
def aggregations():
    """
    عرض أسماء جميع Aggregation Reports.
    """

    try:
        names = (
            get_aggregation_names()
        )

        return {
            "count": len(
                names
            ),
            "aggregations": names,
        }

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=str(
                error
            ),
        )


# ============================================================
# GET /aggregations/{name}
# ============================================================

@app.get(
    "/aggregations/{name}",
    tags=["Phase 2 - Aggregations"],
)
def execute_aggregation(
    name: str,
    limit: int = 20,
):
    """
    تشغيل Aggregation Report مستقل.
    """

    available_aggregations = (
        get_aggregation_names()
    )

    if (
        name
        not in available_aggregations
    ):

        raise HTTPException(
            status_code=404,
            detail={
                "message": (
                    f"Unknown aggregation: {name}"
                ),
                "available_aggregations": (
                    available_aggregations
                ),
            },
        )

    if limit < 1:

        raise HTTPException(
            status_code=400,
            detail=(
                "limit must be greater than 0."
            ),
        )

    try:
        result = run_aggregation(
            name=name,
            limit=limit,
        )

        return make_json_safe(
            result
        )

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=str(
                error
            ),
        )


# ============================================================
# POST /refresh-mv
# ============================================================

@app.post(
    "/refresh-mv",
    tags=["Phase 2 - Materialized Views"],
)
def refresh_materialized_views(
    request: RefreshMVRequest | None = None,
):
    """
    تحديث Materialized Views.

    بدون view_name:
        يحدث الجميع.

    مع view_name:
        يحدث View واحدة فقط.

    force_rebuild=False:
        يستخدم Incremental Refresh.
    """

    try:

        if request is None:

            request = (
                RefreshMVRequest()
            )

        # ====================================================
        # Refresh View واحدة
        # ====================================================

        if request.view_name:

            available_views = list(
                list_views().keys()
            )

            if (
                request.view_name
                not in available_views
            ):

                raise HTTPException(
                    status_code=404,
                    detail={
                        "message": (
                            "Unknown Materialized View."
                        ),
                        "available_views": (
                            available_views
                        ),
                    },
                )

            result = refresh_view(
                request.view_name,
                force_rebuild=(
                    request.force_rebuild
                ),
            )

        # ====================================================
        # Refresh جميع Views
        # ====================================================

        else:

            result = refresh_all(
                force_rebuild=(
                    request.force_rebuild
                )
            )

        return make_json_safe(
            result
        )

    except HTTPException:
        raise

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=str(
                error
            ),
        )


# ============================================================
# GET /jobs
# ============================================================

@app.get(
    "/jobs",
    tags=["Phase 2 - Scheduled Jobs"],
)
def jobs():
    """
    عرض جميع Scheduled Jobs وحالتها.
    """

    try:
        result = get_jobs()

        return make_json_safe(
            result
        )

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=str(
                error
            ),
        )


# ============================================================
# POST /jobs/{name}/run
# ============================================================

@app.post(
    "/jobs/{name}/run",
    tags=["Phase 2 - Scheduled Jobs"],
)
def execute_job(
    name: str,
):
    """
    تشغيل Job يدويًا.

    هذا يحقق شرط إمكانية تشغيل
    Scheduled Jobs يدويًا.
    """

    try:

        jobs_data = get_jobs()

        available_jobs = [
            job[
                "name"
            ]
            for job in jobs_data.get(
                "jobs",
                []
            )
        ]

        # ----------------------------------------------------
        # التحقق من اسم Job
        # ----------------------------------------------------

        if name not in available_jobs:

            raise HTTPException(
                status_code=404,
                detail={
                    "message": (
                        f"Unknown job: {name}"
                    ),
                    "available_jobs": (
                        available_jobs
                    ),
                },
            )

        # ----------------------------------------------------
        # تشغيل Job
        # ----------------------------------------------------

        result = run_job(
            name
        )

        # ----------------------------------------------------
        # في حالة فشل Job داخليًا
        # ----------------------------------------------------

        if (
            result.get(
                "status"
            )
            == "FAILED"
        ):

            raise HTTPException(
                status_code=500,
                detail=make_json_safe(
                    result
                ),
            )

        return make_json_safe(
            result
        )

    except HTTPException:
        raise

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=str(
                error
            ),
        )