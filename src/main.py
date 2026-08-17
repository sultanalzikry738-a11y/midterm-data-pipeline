import argparse
import subprocess
import sys
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

from config.settings import BATCH_SIZE
from src.file_router import route_file


# =========================================================
# PRINT HEADER
# =========================================================

def print_header():
    print()
    print("=" * 60)
    print("      HYBRID BIG DATA ELT PIPELINE")
    print("=" * 60)
    print()


# =========================================================
# RUN PYTHON BATCH LOADER
# =========================================================

def run_python_batch(input_path: Path):
    """
    Run the Python Batch Raw loader.

    This path is selected automatically
    for files smaller than or equal to
    the configured threshold.
    """

    loader_path = (
        PROJECT_ROOT
        / "src"
        / "batch_loader.py"
    )

    command = [
        sys.executable,
        str(loader_path),
        "--input",
        str(input_path),
        "--batch-size",
        str(BATCH_SIZE),
    ]

    print()
    print("===== PYTHON BATCH PATH =====")
    print(f"Input file : {input_path}")
    print(f"Batch size : {BATCH_SIZE}")
    print()

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "Python Batch loader failed."
        )

    print()
    print(
        "PYTHON BATCH RAW LOAD: PASS"
    )


# =========================================================
# RUN PYSPARK LOADER
# =========================================================

def run_pyspark(input_path: Path):
    """
    Run the PySpark Raw loader.

    This path is selected automatically
    for large files.
    """

    loader_path = (
        PROJECT_ROOT
        / "src"
        / "spark_loader.py"
    )

    command = [
        sys.executable,
        str(loader_path),
        "--input",
        str(input_path),
    ]

    print()
    print("===== PYSPARK PATH =====")
    print(f"Input file : {input_path}")
    print()

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "PySpark Raw loader failed."
        )

    print()
    print(
        "PYSPARK RAW LOAD: PASS"
    )


# =========================================================
# MAIN PIPELINE ROUTER
# =========================================================

def run_pipeline(
    input_path: Path,
    execute: bool = False,
):
    """
    Main automatic Hybrid Router.

    1. Check the file.
    2. Read its size.
    3. Select Python Batch or PySpark.
    4. Optionally execute the selected loader.
    """

    print_header()

    # -----------------------------------------------------
    # FILE VALIDATION
    # -----------------------------------------------------

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    if not input_path.is_file():
        raise ValueError(
            f"Input path is not a file: {input_path}"
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

    # -----------------------------------------------------
    # ROUTER SUMMARY
    # -----------------------------------------------------

    print()
    print(
        "===== AUTOMATIC ROUTER RESULT ====="
    )

    print(
        f"Input file : "
        f"{input_path.name}"
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
        f"{engine}"
    )

    print(
        f"Reason     : "
        f"{route['reason']}"
    )

    # -----------------------------------------------------
    # ROUTE ONLY MODE
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
            "the selected loader."
        )

        print()
        print(
            "HYBRID ROUTER: PASS"
        )

        return route

    # -----------------------------------------------------
    # EXECUTION MODE
    # -----------------------------------------------------

    print()
    print(
        "EXECUTION MODE: ENABLED"
    )

    if engine == "python_batch":

        run_python_batch(
            input_path
        )

    elif engine == "pyspark":

        run_pyspark(
            input_path
        )

    else:

        raise RuntimeError(
            f"Unknown engine: {engine}"
        )

    print()
    print("=" * 60)
    print(
        "RAW INGESTION COMPLETED"
    )
    print("=" * 60)

    return route


# =========================================================
# CLI
# =========================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "Automatic Hybrid Router for "
            "the Big Data Midterm Pipeline."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to the input CSV file.",
    )

    parser.add_argument(
        "--execute",
        action="store_true",
        help=(
            "Actually execute the selected "
            "Raw loader. Without this option, "
            "only the routing decision is shown."
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
        print(
            "MAIN PIPELINE: FAIL"
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