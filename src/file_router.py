import argparse
import sys
from pathlib import Path


# إضافة مجلد المشروع الرئيسي حتى نستطيع استيراد config
PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import SMALL_FILE_THRESHOLD_MB


def get_file_size_mb(file_path: Path) -> float:
    """
    Return file size in megabytes.
    """

    if not file_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {file_path}"
        )

    if not file_path.is_file():
        raise ValueError(
            f"Input path is not a file: {file_path}"
        )

    size_bytes = file_path.stat().st_size

    size_mb = size_bytes / (1024 * 1024)

    return size_mb


def choose_engine(file_size_mb: float) -> str:
    """
    Select processing engine based on file size.
    """

    if file_size_mb <= SMALL_FILE_THRESHOLD_MB:
        return "python_batch"

    return "pyspark"


def route_file(file_path: Path) -> dict:
    """
    Inspect file size and return the routing decision.
    """

    file_size_mb = get_file_size_mb(file_path)

    engine = choose_engine(file_size_mb)

    if engine == "python_batch":
        reason = (
            f"File size ({file_size_mb:.2f} MB) "
            f"is <= threshold "
            f"({SMALL_FILE_THRESHOLD_MB:.2f} MB)."
        )
    else:
        reason = (
            f"File size ({file_size_mb:.2f} MB) "
            f"is > threshold "
            f"({SMALL_FILE_THRESHOLD_MB:.2f} MB)."
        )

    return {
        "file_path": str(file_path.resolve()),
        "file_name": file_path.name,
        "file_size_mb": file_size_mb,
        "threshold_mb": SMALL_FILE_THRESHOLD_MB,
        "engine": engine,
        "reason": reason,
    }


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Automatically select Python Batch or PySpark "
            "based on input file size."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to the input CSV file.",
    )

    return parser.parse_args()


def main():
    args = parse_arguments()

    print("===== FILE ROUTER =====")

    try:
        decision = route_file(args.input)

        print(f"File name      : {decision['file_name']}")
        print(
            f"File size MB   : "
            f"{decision['file_size_mb']:.2f}"
        )
        print(
            f"Threshold MB   : "
            f"{decision['threshold_mb']:.2f}"
        )
        print(f"Selected engine: {decision['engine']}")
        print(f"Reason         : {decision['reason']}")

        print("FILE ROUTER: PASS")

    except Exception as error:
        print("FILE ROUTER: FAIL")
        print(f"ERROR: {error}")
        raise


if __name__ == "__main__":
    main()