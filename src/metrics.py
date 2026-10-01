import json
import sys
from datetime import datetime, timezone
from pathlib import Path


# =========================================================
# PROJECT ROOT
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# =========================================================
# PROJECT IMPORTS
# =========================================================

from config.settings import RESULTS_JSON


# =========================================================
# تحويل القيم إلى صيغة مناسبة لـ JSON
# =========================================================

def make_json_safe(value):
    """
    تحويل بعض أنواع Python التي لا يقبلها JSON مباشرة.
    """

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, dict):
        return {
            key: make_json_safe(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple, set)):
        return [
            make_json_safe(item)
            for item in value
        ]

    return value


# =========================================================
# بناء مقاييس التشغيل النهائية
# =========================================================

def build_results(
    input_path: Path,
    route: dict,
    raw_metrics: dict,
    elt_metrics: dict,
) -> dict:
    """
    دمج نتائج:
    Router + Raw Loader + ELT
    في تقرير نهائي واحد.
    """

    input_path = Path(input_path)

    # -----------------------------------------------------
    # المحرك المستخدم
    # -----------------------------------------------------

    engine_used = (
        route.get("engine")
        or raw_metrics.get("engine_used")
    )

    # -----------------------------------------------------
    # الزمن الكلي
    # -----------------------------------------------------

    raw_seconds = float(
        raw_metrics.get(
            "elapsed_seconds",
            0.0,
        )
    )

    elt_seconds = float(
        elt_metrics.get(
            "elapsed_seconds",
            0.0,
        )
    )

    total_seconds = (
        raw_seconds
        + elt_seconds
    )

    # -----------------------------------------------------
    # العدادات
    # -----------------------------------------------------

    read_rows = int(
        raw_metrics.get(
            "rows_read",
            0,
        )
    )

    loaded_raw = int(
        raw_metrics.get(
            "rows_loaded",
            0,
        )
    )

    count_valid = int(
        elt_metrics.get(
            "valid_count",
            0,
        )
    )

    count_corrected = int(
        elt_metrics.get(
            "corrected_count",
            0,
        )
    )

    count_quarantine = int(
        elt_metrics.get(
            "quarantine_count",
            0,
        )
    )

    terminal_count = (
        count_valid
        + count_corrected
        + count_quarantine
    )

    # -----------------------------------------------------
    # قاعدة الاتساق المطلوبة من الدكتور
    # -----------------------------------------------------

    consistency_pass = (
        loaded_raw
        == terminal_count
    )

    # -----------------------------------------------------
    # Throughput النهائي
    # -----------------------------------------------------

    throughput = (
        terminal_count / total_seconds
        if total_seconds > 0
        else 0.0
    )

    # -----------------------------------------------------
    # إعدادات المحرك
    # -----------------------------------------------------

    engine_settings = {}

    if engine_used == "python_batch":

        engine_settings[
            "batch_size"
        ] = raw_metrics.get(
            "batch_size"
        )

        engine_settings[
            "batch_count"
        ] = raw_metrics.get(
            "batch_count"
        )

    elif engine_used == "pyspark":

        engine_settings[
            "partitions"
        ] = raw_metrics.get(
            "input_partitions"
        )

        engine_settings[
            "spark_version"
        ] = raw_metrics.get(
            "spark_version"
        )

        engine_settings[
            "spark_master"
        ] = raw_metrics.get(
            "spark_master"
        )

    # -----------------------------------------------------
    # التقرير النهائي
    # -----------------------------------------------------

    return {

        "run_id": raw_metrics.get(
            "run_id"
        ),

        "file_name": input_path.name,

        "file_size_mb": round(
            float(
                route.get(
                    "file_size_mb",
                    0.0,
                )
            ),
            2,
        ),

        "engine_used": engine_used,

        "read_rows": read_rows,

        "loaded_raw": loaded_raw,

        "count_valid": count_valid,

        "count_corrected": (
            count_corrected
        ),

        "count_quarantine": (
            count_quarantine
        ),

        "seconds_elapsed": round(
            total_seconds,
            2,
        ),

        "throughput": round(
            throughput,
            2,
        ),

        "engine_settings": (
            engine_settings
        ),

        "counts_case_error": (
            elt_metrics.get(
                "reason_counts",
                {},
            )
        ),

        "count_inserted": int(
            elt_metrics.get(
                "inserted_count",
                0,
            )
        ),

        "count_updated": int(
            elt_metrics.get(
                "updated_count",
                0,
            )
        ),

        "count_unchanged": int(
            elt_metrics.get(
                "unchanged_count",
                0,
            )
        ),

        "consistency_check": {

            "formula": (
                "loaded_raw = "
                "count_valid + "
                "count_corrected + "
                "count_quarantine"
            ),

            "loaded_raw": (
                loaded_raw
            ),

            "terminal_count": (
                terminal_count
            ),

            "status": (
                "PASS"
                if consistency_pass
                else "FAIL"
            ),
        },

        "generated_at": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
    }


# =========================================================
# حفظ النتائج في reports/results.json
# =========================================================
def save_results(
    results: dict,
    output_path: Path = RESULTS_JSON,
) -> Path:
    """
    حفظ نتائج التشغيل.
    
    إذا كان results.json موجودًا من تشغيل سابق،
    يتم حفظ التشغيل الجديد في ملف منفصل حتى لا
    تضيع نتائج التشغيل السابقة.
    """

    output_path = Path(output_path)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # إذا كان الملف موجودًا نحفظ نسخة جديدة
    if output_path.exists():

        run_id = results.get(
            "run_id",
            "unknown_run"
        )

        output_path = (
            output_path.parent
            /
            f"results_{run_id}.json"
        )

    safe_results = make_json_safe(
        results
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            safe_results,
            file,
            ensure_ascii=False,
            indent=2,
        )

    return output_path

    output_path = Path(
        output_path
    )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    safe_results = make_json_safe(
        results
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            safe_results,
            file,
            ensure_ascii=False,
            indent=2,
        )

    return output_path


# =========================================================
# طباعة ملخص المقاييس
# =========================================================

def print_results_summary(
    results: dict,
):
    """
    طباعة أهم المقاييس بعد انتهاء التشغيل.
    """

    print()
    print(
        "===== PIPELINE METRICS ====="
    )

    print(
        f"Run ID       : "
        f"{results.get('run_id')}"
    )

    print(
        f"File         : "
        f"{results.get('file_name')}"
    )

    print(
        f"Engine       : "
        f"{results.get('engine_used')}"
    )

    print(
        f"Rows read    : "
        f"{results.get('read_rows')}"
    )

    print(
        f"Raw loaded   : "
        f"{results.get('loaded_raw')}"
    )

    print(
        f"Valid        : "
        f"{results.get('count_valid')}"
    )

    print(
        f"Corrected    : "
        f"{results.get('count_corrected')}"
    )

    print(
        f"Quarantine   : "
        f"{results.get('count_quarantine')}"
    )

    print(
        f"Inserted     : "
        f"{results.get('count_inserted')}"
    )

    print(
        f"Updated      : "
        f"{results.get('count_updated')}"
    )

    print(
        f"Unchanged    : "
        f"{results.get('count_unchanged')}"
    )

    print(
        f"Seconds      : "
        f"{results.get('seconds_elapsed')}"
    )

    print(
        f"Throughput   : "
        f"{results.get('throughput')} rows/sec"
    )

    print(
        f"Consistency  : "
        f"{results.get('consistency_check', {}).get('status')}"
    )

    print()

    print(
        f"Results file : "
        f"{RESULTS_JSON}"
    )