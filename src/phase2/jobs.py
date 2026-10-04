# ============================================================
# Phase 2 - Scheduled Jobs
# ============================================================
#
# هذا الملف مسؤول عن الـ Scheduled Jobs المطلوبة في Phase 2.
#
# لدينا Jobان رئيسيان:
#
# 1) refresh_materialized_views
#    يقوم بتحديث الـ Materialized Views.
#
# 2) export_materialized_views_report
#    يقوم بإنشاء تقرير JSON من الـ Materialized Views.
#
# كل Job:
# - يمكن تشغيله يدويًا.
# - له Schedule واضح.
# - يسجل وقت البداية والنهاية.
# - يسجل SUCCESS أو FAILED.
# - يسجل مدة التنفيذ.
#
# لاحقًا FastAPI سيستدعي نفس الدوال الموجودة هنا.
# ============================================================

import argparse
import json
import time
import traceback

from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DB_NAME,
)

from src.phase2.materialized_views import (
    refresh_all,
    DAILY_VIEW,
    CITY_VIEW,
)


# ============================================================
# المسارات
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

JOBS_REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "phase2"
    / "jobs"
)

LOG_FILE = (
    JOBS_REPORT_DIR
    / "job_execution_log.jsonl"
)

STATE_FILE = (
    JOBS_REPORT_DIR
    / "job_state.json"
)

MV_REPORT_FILE = (
    JOBS_REPORT_DIR
    / "materialized_views_report_latest.json"
)


# ============================================================
# تعريف الـ Jobs
# ============================================================

JOB_DEFINITIONS = {
    "refresh_materialized_views": {
        "description": (
            "تحديث جميع Materialized Views "
            "باستخدام Incremental Refresh."
        ),
        "schedule": "Every 1 hour",
        "interval_seconds": 3600,
    },

    "export_materialized_views_report": {
        "description": (
            "إنشاء تقرير دوري من "
            "daily_sales_summary و city_sales_summary."
        ),
        "schedule": "Every 24 hours",
        "interval_seconds": 86400,
    },
}


# ============================================================
# دوال الوقت و JSON
# ============================================================

def utc_now():
    """
    إرجاع الوقت الحالي UTC.
    """
    return datetime.now(timezone.utc)


def iso_now():
    """
    الوقت الحالي كنص ISO.
    """
    return utc_now().isoformat()


def json_default(value):
    """
    تحويل القيم غير المدعومة مباشرة في JSON.
    """

    if isinstance(value, datetime):
        return value.isoformat()

    return str(value)


def print_json(data):
    """
    طباعة JSON مع دعم العربية.
    """

    print(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            default=json_default,
        )
    )


# ============================================================
# تجهيز مجلد التقارير
# ============================================================

def ensure_jobs_directory():
    """
    إنشاء مجلد تقارير الـ Jobs إذا لم يكن موجودًا.
    """

    JOBS_REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


# ============================================================
# قراءة وحفظ حالة الـ Jobs
# ============================================================

def load_state():
    """
    قراءة آخر حالة تشغيل لكل Job.
    """

    ensure_jobs_directory()

    if not STATE_FILE.exists():
        return {
            "jobs": {}
        }

    try:
        with STATE_FILE.open(
            "r",
            encoding="utf-8",
        ) as file:
            return json.load(file)

    except (
        json.JSONDecodeError,
        OSError,
    ):
        return {
            "jobs": {}
        }


def save_state(state):
    """
    حفظ حالة الـ Jobs.
    """

    ensure_jobs_directory()

    with STATE_FILE.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            state,
            file,
            ensure_ascii=False,
            indent=2,
            default=json_default,
        )


# ============================================================
# Logging
# ============================================================

def write_log(record):
    """
    تسجيل كل حدث في ملف JSONL.

    كل سطر يمثل Event مستقل.
    """

    ensure_jobs_directory()

    with LOG_FILE.open(
        "a",
        encoding="utf-8",
    ) as file:
        file.write(
            json.dumps(
                record,
                ensure_ascii=False,
                default=json_default,
            )
        )

        file.write("\n")


# ============================================================
# Job رقم 1
# Refresh Materialized Views
# ============================================================

def job_refresh_materialized_views():
    """
    تحديث جميع الـ Materialized Views.

    إذا لا توجد تغييرات:
        NO_CHANGES

    إذا توجد بيانات جديدة:
        INCREMENTAL

    ولا نستخدم Full Rebuild إلا عند الحاجة.
    """

    result = refresh_all(
        force_rebuild=False
    )

    return {
        "job": "refresh_materialized_views",
        "action": "refresh_all_materialized_views",
        "result": result,
    }


# ============================================================
# Job رقم 2
# Export Materialized Views Report
# ============================================================

def job_export_materialized_views_report():
    """
    إنشاء تقرير JSON دوري من الـ Materialized Views.

    هذا التقرير لا يعيد قراءة 27 مليون سجل.

    بل يقرأ الملخصات الجاهزة:
    - daily_sales_summary
    - city_sales_summary
    """

    ensure_jobs_directory()

    client = MongoClient(
        MONGO_URI
    )

    db = client[
        MONGO_DB_NAME
    ]

    try:
        daily_results = list(
            db[
                DAILY_VIEW
            ].find(
                {},
                {
                    "_id": 0,
                },
            ).sort(
                "period",
                -1,
            )
        )

        city_results = list(
            db[
                CITY_VIEW
            ].find(
                {},
                {
                    "_id": 0,
                },
            ).sort(
                "total_sales",
                -1,
            )
        )

        report = {
            "report_name": (
                "materialized_views_periodic_report"
            ),
            "database": MONGO_DB_NAME,
            "generated_at": iso_now(),
            "daily_sales_summary": {
                "document_count": len(
                    daily_results
                ),
                "results": daily_results,
            },
            "city_sales_summary": {
                "document_count": len(
                    city_results
                ),
                "results": city_results,
            },
        }

        with MV_REPORT_FILE.open(
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                report,
                file,
                ensure_ascii=False,
                indent=2,
                default=json_default,
            )

        return {
            "job": (
                "export_materialized_views_report"
            ),
            "action": (
                "export_materialized_views_report"
            ),
            "status": "SUCCESS",
            "daily_documents": len(
                daily_results
            ),
            "city_documents": len(
                city_results
            ),
            "saved_to": str(
                MV_REPORT_FILE
            ),
        }

    finally:
        client.close()


# ============================================================
# تنفيذ Job بالاسم
# ============================================================

def execute_job_function(
    job_name,
):
    """
    ربط اسم الـ Job بالدالة الحقيقية.
    """

    if (
        job_name
        == "refresh_materialized_views"
    ):
        return (
            job_refresh_materialized_views()
        )

    if (
        job_name
        == "export_materialized_views_report"
    ):
        return (
            job_export_materialized_views_report()
        )

    raise ValueError(
        f"Unknown job: {job_name}"
    )


# ============================================================
# تشغيل Job مع Logging
# ============================================================

def run_job(job_name):
    """
    تشغيل Job يدويًا أو من Scheduler.

    يتم تسجيل:
    - وقت البداية
    - وقت النهاية
    - مدة التنفيذ
    - SUCCESS / FAILED
    """

    if job_name not in JOB_DEFINITIONS:
        raise ValueError(
            f"Unknown job: {job_name}"
        )

    start_time = utc_now()

    start_record = {
        "event": "JOB_START",
        "job_name": job_name,
        "started_at": start_time.isoformat(),
    }

    write_log(
        start_record
    )

    state = load_state()

    state.setdefault(
        "jobs",
        {}
    )

    state[
        "jobs"
    ].setdefault(
        job_name,
        {}
    )

    state[
        "jobs"
    ][
        job_name
    ][
        "last_started_at"
    ] = start_time.isoformat()

    state[
        "jobs"
    ][
        job_name
    ][
        "last_status"
    ] = "RUNNING"

    save_state(
        state
    )

    try:
        job_result = (
            execute_job_function(
                job_name
            )
        )

        end_time = utc_now()

        duration_seconds = round(
            (
                end_time
                - start_time
            ).total_seconds(),
            3,
        )

        result = {
            "job_name": job_name,
            "status": "SUCCESS",
            "started_at": (
                start_time.isoformat()
            ),
            "finished_at": (
                end_time.isoformat()
            ),
            "duration_seconds": (
                duration_seconds
            ),
            "result": job_result,
        }

        write_log(
            {
                "event": "JOB_END",
                **result,
            }
        )

        state = load_state()

        state.setdefault(
            "jobs",
            {}
        )

        state[
            "jobs"
        ].setdefault(
            job_name,
            {}
        )

        state[
            "jobs"
        ][
            job_name
        ][
            "last_finished_at"
        ] = end_time.isoformat()

        state[
            "jobs"
        ][
            job_name
        ][
            "last_status"
        ] = "SUCCESS"

        state[
            "jobs"
        ][
            job_name
        ][
            "last_duration_seconds"
        ] = duration_seconds

        save_state(
            state
        )

        return result

    except Exception as error:
        end_time = utc_now()

        duration_seconds = round(
            (
                end_time
                - start_time
            ).total_seconds(),
            3,
        )

        result = {
            "job_name": job_name,
            "status": "FAILED",
            "started_at": (
                start_time.isoformat()
            ),
            "finished_at": (
                end_time.isoformat()
            ),
            "duration_seconds": (
                duration_seconds
            ),
            "error": str(
                error
            ),
            "traceback": traceback.format_exc(),
        }

        write_log(
            {
                "event": "JOB_END",
                **result,
            }
        )

        state = load_state()

        state.setdefault(
            "jobs",
            {}
        )

        state[
            "jobs"
        ].setdefault(
            job_name,
            {}
        )

        state[
            "jobs"
        ][
            job_name
        ][
            "last_finished_at"
        ] = end_time.isoformat()

        state[
            "jobs"
        ][
            job_name
        ][
            "last_status"
        ] = "FAILED"

        state[
            "jobs"
        ][
            job_name
        ][
            "last_error"
        ] = str(
            error
        )

        save_state(
            state
        )

        return result


# ============================================================
# عرض الـ Jobs
# ============================================================

def get_jobs():
    """
    عرض جميع الـ Jobs مع الـ Schedule وحالة آخر تشغيل.
    """

    state = load_state()

    jobs_state = state.get(
        "jobs",
        {}
    )

    results = []

    for (
        job_name,
        definition,
    ) in JOB_DEFINITIONS.items():

        current_state = jobs_state.get(
            job_name,
            {},
        )

        results.append(
            {
                "name": job_name,
                "description": (
                    definition[
                        "description"
                    ]
                ),
                "schedule": (
                    definition[
                        "schedule"
                    ]
                ),
                "interval_seconds": (
                    definition[
                        "interval_seconds"
                    ]
                ),
                "last_started_at": (
                    current_state.get(
                        "last_started_at"
                    )
                ),
                "last_finished_at": (
                    current_state.get(
                        "last_finished_at"
                    )
                ),
                "last_status": (
                    current_state.get(
                        "last_status"
                    )
                ),
                "last_duration_seconds": (
                    current_state.get(
                        "last_duration_seconds"
                    )
                ),
            }
        )

    return {
        "job_count": len(
            results
        ),
        "jobs": results,
    }


# ============================================================
# معرفة إذا كان Job مستحق التشغيل
# ============================================================

def parse_datetime(
    value,
):
    """
    تحويل ISO String إلى datetime.
    """

    if not value:
        return None

    try:
        return datetime.fromisoformat(
            value
        )

    except ValueError:
        return None


def is_job_due(
    job_name,
):
    """
    فحص هل حان وقت تشغيل Job.
    """

    definition = JOB_DEFINITIONS[
        job_name
    ]

    interval_seconds = definition[
        "interval_seconds"
    ]

    state = load_state()

    job_state = (
        state
        .get(
            "jobs",
            {},
        )
        .get(
            job_name,
            {},
        )
    )

    last_finished_at = parse_datetime(
        job_state.get(
            "last_finished_at"
        )
    )

    # إذا لم يعمل من قبل
    # فهو مستحق التشغيل.
    if last_finished_at is None:
        return True

    elapsed_seconds = (
        utc_now()
        - last_finished_at
    ).total_seconds()

    return (
        elapsed_seconds
        >= interval_seconds
    )


# ============================================================
# تشغيل الـ Jobs المستحقة فقط
# ============================================================

def run_due_jobs():
    """
    تشغيل الـ Jobs التي وصل موعدها فقط.
    """

    results = []

    for job_name in JOB_DEFINITIONS:

        if is_job_due(
            job_name
        ):
            results.append(
                run_job(
                    job_name
                )
            )

    return {
        "checked_at": iso_now(),
        "executed_count": len(
            results
        ),
        "results": results,
    }


# ============================================================
# Scheduler Loop
# ============================================================

def run_scheduler(
    poll_seconds=60,
):
    """
    Scheduler بسيط.

    يفحص كل فترة هل توجد Jobs مستحقة.

    يمكن إيقافه بـ:
    Ctrl + C
    """

    print(
        "Phase 2 Scheduler started."
    )

    print(
        f"Polling every {poll_seconds} seconds."
    )

    print(
        "Press Ctrl+C to stop."
    )

    try:
        while True:

            result = run_due_jobs()

            print_json(
                result
            )

            time.sleep(
                poll_seconds
            )

    except KeyboardInterrupt:
        print(
            "\nScheduler stopped."
        )


# ============================================================
# عرض آخر Logs
# ============================================================

def get_recent_logs(
    limit=20,
):
    """
    قراءة آخر Logs.
    """

    ensure_jobs_directory()

    if not LOG_FILE.exists():
        return []

    with LOG_FILE.open(
        "r",
        encoding="utf-8",
    ) as file:

        lines = file.readlines()

    lines = lines[
        -limit:
    ]

    results = []

    for line in lines:

        try:
            results.append(
                json.loads(
                    line
                )
            )

        except json.JSONDecodeError:
            continue

    return results


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Phase 2 Scheduled Jobs Manager"
        )
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help="List all jobs.",
    )

    parser.add_argument(
        "--run",
        choices=list(
            JOB_DEFINITIONS.keys()
        ),
        help="Run one job manually.",
    )

    parser.add_argument(
        "--run-due",
        action="store_true",
        help="Run jobs whose schedule is due.",
    )

    parser.add_argument(
        "--scheduler",
        action="store_true",
        help="Start the scheduler loop.",
    )

    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=60,
        help=(
            "Scheduler polling interval "
            "in seconds."
        ),
    )

    parser.add_argument(
        "--logs",
        action="store_true",
        help="Show recent job logs.",
    )

    args = parser.parse_args()

    if args.list:
        print_json(
            get_jobs()
        )

        return

    if args.run:
        result = run_job(
            args.run
        )

        print_json(
            result
        )

        if (
            result.get(
                "status"
            )
            == "FAILED"
        ):
            raise SystemExit(
                1
            )

        return

    if args.run_due:
        print_json(
            run_due_jobs()
        )

        return

    if args.scheduler:
        run_scheduler(
            poll_seconds=max(
                args.poll_seconds,
                1,
            )
        )

        return

    if args.logs:
        print_json(
            get_recent_logs()
        )

        return

    parser.print_help()


if __name__ == "__main__":
    main()