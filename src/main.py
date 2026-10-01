import argparse
import sys
import uuid
from pathlib import Path


# =========================================================
# PROJECT ROOT
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(
        0,
        str(PROJECT_ROOT),
    )


# =========================================================
# PROJECT IMPORTS
# =========================================================

from config.settings import BATCH_SIZE
from src.file_router import route_file
from src.metrics import (
    build_results,
    save_results,
    print_results_summary,
)


# =========================================================
# PRINT HEADER
# =========================================================

def print_header():
    """
    طباعة عنوان المشروع عند بداية التشغيل.
    """

    print()
    print("=" * 60)
    print("      HYBRID BIG DATA ELT PIPELINE")
    print("=" * 60)
    print()


# =========================================================
# PRINT ROUTER RESULT
# =========================================================

def print_route_result(
    input_path: Path,
    route: dict,
):
    """
    طباعة قرار الـ Router وسبب الاختيار.
    """

    print()
    print(
        "===== AUTOMATIC ROUTER RESULT ====="
    )

    print(
        f"Input file : {input_path.name}"
    )

    print(
        f"File size  : "
        f"{route['file_size_mb']:.2f} MB"
    )

    print(
        f"Threshold  : "
        f"{route['threshold_mb']:.2f} MB"
    )

    print(
        f"Engine     : "
        f"{route['engine']}"
    )

    print(
        f"Reason     : "
        f"{route['reason']}"
    )


# =========================================================
# PYTHON BATCH FULL PATH
# =========================================================

def run_python_path(
    input_path: Path,
    run_id: str,
):
    """
    تشغيل المسار الكامل للملفات الصغيرة:

    CSV
    -> Python Batch Raw Load
    -> MongoDB orders_raw
    -> Cleaning + Classification
    -> Upsert / Quarantine
    """

    # الاستيراد هنا حتى لا نحمل الملفات
    # إلا عندما يختار Router هذا المسار.
    from src.batch_loader import (
        load_csv_to_raw,
    )

    from src.elt_pipeline import (
        process_raw_run,
    )

    print()
    print("=" * 60)
    print(
        "PYTHON BATCH FULL ELT PATH"
    )
    print("=" * 60)

    print(
        f"Run ID     : {run_id}"
    )

    print(
        f"Input file : {input_path}"
    )

    print(
        f"Batch size : {BATCH_SIZE}"
    )

    # -----------------------------------------------------
    # STAGE 1 - RAW LOAD
    # -----------------------------------------------------

    print()
    print(
        "===== STAGE 1: RAW LOAD ====="
    )

    raw_metrics = load_csv_to_raw(
        input_path=input_path,
        batch_size=BATCH_SIZE,
        run_id=run_id,
    )

    print()
    print(
        "PYTHON BATCH RAW LOAD: PASS"
    )

    print(
        f"Rows read   : "
        f"{raw_metrics['rows_read']}"
    )

    print(
        f"Rows loaded : "
        f"{raw_metrics['rows_loaded']}"
    )

    # التأكد أن كل السجلات المقروءة وصلت إلى Raw
    if (
        raw_metrics["rows_read"]
        != raw_metrics["rows_loaded"]
    ):
        raise RuntimeError(
            "Python Raw Load consistency failed: "
            "rows_read != rows_loaded."
        )

    # -----------------------------------------------------
    # STAGE 2 - QUALITY + ELT
    # -----------------------------------------------------

    print()
    print(
        "===== STAGE 2: QUALITY + ELT ====="
    )

    elt_metrics = process_raw_run(
        run_id=run_id,
        progress_every=BATCH_SIZE,
    )

    # التأكد من قاعدة الاتساق المطلوبة
    if not elt_metrics.get(
        "consistency_pass",
        False,
    ):
        raise RuntimeError(
            "Python ELT consistency check failed."
        )

    print()
    print(
        "PYTHON END-TO-END ELT: PASS"
    )

    return (
        raw_metrics,
        elt_metrics,
    )


# =========================================================
# PYSPARK FULL PATH
# =========================================================

def run_pyspark_path(
    input_path: Path,
    run_id: str,
):
    """
    تشغيل المسار الكامل للملفات الكبيرة:

    CSV
    -> PySpark Raw Load
    -> MongoDB orders_raw
    -> Spark Quality + Classification
    -> Upsert / Quarantine
    """

    # الاستيراد هنا حتى لا يبدأ Spark
    # إلا عندما يختار Router مسار PySpark.
    from src.spark_loader import (
        load_csv_to_raw_with_spark,
    )

    from src.spark_elt_pipeline import (
        run_spark_elt,
    )

    print()
    print("=" * 60)
    print(
        "PYSPARK FULL ELT PATH"
    )
    print("=" * 60)

    print(
        f"Run ID     : {run_id}"
    )

    print(
        f"Input file : {input_path}"
    )

    # -----------------------------------------------------
    # STAGE 1 - RAW LOAD
    # -----------------------------------------------------

    print()
    print(
        "===== STAGE 1: SPARK RAW LOAD ====="
    )

    raw_metrics = (
        load_csv_to_raw_with_spark(
            input_path=input_path,
            run_id=run_id,
        )
    )

    print()
    print(
        "PYSPARK RAW LOAD: PASS"
    )

    print(
        f"Rows read   : "
        f"{raw_metrics['rows_read']}"
    )

    print(
        f"Rows loaded : "
        f"{raw_metrics['rows_loaded']}"
    )

    print(
        f"Partitions  : "
        f"{raw_metrics['input_partitions']}"
    )

    # التأكد أن كل السجلات وصلت إلى Raw
    if (
        raw_metrics["rows_read"]
        != raw_metrics["rows_loaded"]
    ):
        raise RuntimeError(
            "Spark Raw Load consistency failed: "
            "rows_read != rows_loaded."
        )

    # -----------------------------------------------------
    # STAGE 2 - SPARK QUALITY + ELT
    # -----------------------------------------------------

    print()
    print(
        "===== STAGE 2: SPARK QUALITY + ELT ====="
    )

    elt_metrics = run_spark_elt(
        run_id=run_id,
        dry_run=False,
        limit=None,
        show_plan=False,
    )

    # النسخة التي عدلناها يجب أن ترجع Metrics
    if elt_metrics is None:
        raise RuntimeError(
            "Spark ELT did not return metrics. "
            "Check spark_elt_pipeline.py."
        )

    # التأكد من قاعدة الاتساق
    if not elt_metrics.get(
        "consistency_pass",
        False,
    ):
        raise RuntimeError(
            "Spark ELT consistency check failed."
        )

    print()
    print(
        "PYSPARK END-TO-END ELT: PASS"
    )

    return (
        raw_metrics,
        elt_metrics,
    )


# =========================================================
# MAIN HYBRID PIPELINE
# =========================================================

def run_pipeline(
    input_path: Path,
    execute: bool = False,
):
    """
    نقطة التشغيل الرئيسية للمشروع.

    الخطوات:

    1. التحقق من الملف.
    2. قراءة حجم الملف.
    3. اختيار المحرك تلقائياً.
    4. إنشاء run_id واحد للعملية.
    5. تحميل البيانات إلى Raw.
    6. تنفيذ التنظيف والتصنيف.
    7. الكتابة إلى Validated / Quarantine.
    8. حفظ Metrics في reports/results.json.
    """

    print_header()

    # -----------------------------------------------------
    # FILE VALIDATION
    # -----------------------------------------------------

    if not input_path.exists():

        raise FileNotFoundError(
            f"Input file not found: "
            f"{input_path}"
        )

    if not input_path.is_file():

        raise ValueError(
            f"Input path is not a file: "
            f"{input_path}"
        )

    # -----------------------------------------------------
    # AUTOMATIC ROUTING
    # -----------------------------------------------------

    route = route_file(
        input_path
    )

    engine = route[
        "engine"
    ]

    print_route_result(
        input_path=input_path,
        route=route,
    )

    # -----------------------------------------------------
    # ROUTER CHECK-ONLY MODE
    # -----------------------------------------------------

    if not execute:

        print()
        print(
            "ROUTER MODE: CHECK ONLY"
        )

        print(
            "No data was written."
        )

        print(
            "Use --execute to run "
            "the complete ELT pipeline."
        )

        print()
        print(
            "HYBRID ROUTER: PASS"
        )

        return {
            "route": route,
        }

    # -----------------------------------------------------
    # CREATE ONE RUN ID
    # -----------------------------------------------------

    run_id = str(
        uuid.uuid4()
    )

    print()
    print(
        "===== EXECUTION MODE ====="
    )

    print(
        f"Run ID : {run_id}"
    )

    print(
        f"Engine : {engine}"
    )

    # -----------------------------------------------------
    # SELECT ENGINE
    # -----------------------------------------------------

    if engine == "python_batch":

        (
            raw_metrics,
            elt_metrics,
        ) = run_python_path(
            input_path=input_path,
            run_id=run_id,
        )

    elif engine == "pyspark":

        (
            raw_metrics,
            elt_metrics,
        ) = run_pyspark_path(
            input_path=input_path,
            run_id=run_id,
        )

    else:

        raise RuntimeError(
            f"Unknown engine: {engine}"
        )

    # -----------------------------------------------------
    # FINAL METRICS
    # -----------------------------------------------------

    print()
    print(
        "===== STAGE 3: METRICS ====="
    )

    results = build_results(
        input_path=input_path,
        route=route,
        raw_metrics=raw_metrics,
        elt_metrics=elt_metrics,
    )

    results_path = save_results(
        results
    )

    print_results_summary(
        results
    )

    print(
        f"Metrics saved to: "
        f"{results_path}"
    )

    # -----------------------------------------------------
    # FINAL CONSISTENCY
    # -----------------------------------------------------

    consistency_status = (
        results
        .get(
            "consistency_check",
            {},
        )
        .get(
            "status"
        )
    )

    if consistency_status != "PASS":

        raise RuntimeError(
            "Final pipeline consistency "
            "check failed."
        )

    # -----------------------------------------------------
    # SUCCESS
    # -----------------------------------------------------

    print()
    print("=" * 60)
    print(
        "HYBRID BIG DATA ELT PIPELINE: PASS"
    )
    print("=" * 60)

    print(
        f"Run ID : {run_id}"
    )

    print(
        f"Engine : {engine}"
    )

    print(
        f"Results: {results_path}"
    )

    return {
        "run_id": run_id,
        "route": route,
        "raw_metrics": raw_metrics,
        "elt_metrics": elt_metrics,
        "results": results,
    }


# =========================================================
# COMMAND LINE ARGUMENTS
# =========================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "Automatic Hybrid Big Data ELT Pipeline. "
            "The Router selects Python Batch or PySpark "
            "according to the input file size."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help=(
            "Path to the input CSV file."
        ),
    )

    parser.add_argument(
        "--execute",
        action="store_true",
        help=(
            "Execute the complete pipeline. "
            "Without this option, only the "
            "Router decision is displayed."
        ),
    )

    return parser.parse_args()


# =========================================================
# MAIN
# =========================================================

def main():

    args = parse_arguments()

    input_path = Path(
        args.input
    ).expanduser().resolve()

    try:

        run_pipeline(
            input_path=input_path,
            execute=args.execute,
        )

    except Exception as error:

        print()
        print("=" * 60)
        print(
            "MAIN PIPELINE: FAIL"
        )
        print("=" * 60)

        print(
            f"ERROR TYPE: "
            f"{type(error).__name__}"
        )

        print(
            f"ERROR: {error}"
        )

        raise


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()