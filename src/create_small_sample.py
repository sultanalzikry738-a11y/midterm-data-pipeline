import argparse
import csv
import sys
import time
from pathlib import Path


# نضيف مجلد المشروع الرئيسي حتى نستطيع استيراد config
PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import SAMPLE_ROWS, SMALL_SAMPLE_FILE


def create_small_sample(
    input_path: Path,
    output_path: Path,
    rows: int,
):
    """
    Create a reproducible small CSV sample by taking
    the first N data rows from the original CSV file.
    """

    if rows <= 0:
        raise ValueError("rows must be greater than 0")

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    if input_path.resolve() == output_path.resolve():
        raise ValueError(
            "Input file and output file cannot be the same."
        )

    # نتأكد أن مجلد الإخراج موجود
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    start_time = time.perf_counter()

    rows_written = 0

    # قراءة Streaming:
    # لا نحمل الملف الكامل إلى الذاكرة
    with input_path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as source_file:

        reader = csv.reader(source_file)

        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("Input CSV file is empty.")

        with output_path.open(
            "w",
            encoding="utf-8-sig",
            newline="",
        ) as output_file:

            writer = csv.writer(output_file)

            # كتابة Header
            writer.writerow(header)

            # كتابة أول N صف فقط
            for row in reader:
                writer.writerow(row)
                rows_written += 1

                if rows_written >= rows:
                    break

    elapsed_seconds = time.perf_counter() - start_time

    output_size_mb = (
        output_path.stat().st_size / (1024 * 1024)
    )

    return {
        "requested_rows": rows,
        "rows_written": rows_written,
        "output_size_mb": output_size_mb,
        "elapsed_seconds": elapsed_seconds,
    }


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Create a reproducible small CSV sample "
            "from the large orders dataset."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to the original large CSV file.",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=SMALL_SAMPLE_FILE,
        help="Path for the generated sample CSV.",
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=SAMPLE_ROWS,
        help="Number of data rows to extract.",
    )

    return parser.parse_args()


def main():
    args = parse_arguments()

    print("===== SMALL SAMPLE CREATION =====")
    print(f"Input file     : {args.input}")
    print(f"Output file    : {args.output}")
    print(f"Requested rows : {args.rows}")

    try:
        result = create_small_sample(
            input_path=args.input,
            output_path=args.output,
            rows=args.rows,
        )

        print()
        print("===== RESULT =====")
        print(
            f"Rows written    : "
            f"{result['rows_written']}"
        )
        print(
            f"Output size MB  : "
            f"{result['output_size_mb']:.2f}"
        )
        print(
            f"Elapsed seconds : "
            f"{result['elapsed_seconds']:.2f}"
        )

        if result["rows_written"] < result["requested_rows"]:
            print(
                "WARNING: Source file contained fewer rows "
                "than requested."
            )

        print("SAMPLE CREATION: PASS")

    except Exception as error:
        print(f"SAMPLE CREATION: FAIL")
        print(f"ERROR: {error}")
        raise


if __name__ == "__main__":
    main()
