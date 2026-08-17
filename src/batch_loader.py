import argparse
import csv
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pymongo.errors import PyMongoError


# =========================================================
# PROJECT IMPORTS
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import BATCH_SIZE
from src.mongo_setup import (
    get_mongo_client,
    get_database,
    get_raw_collection,
)


# =========================================================
# RAW DOCUMENT BUILDER
# =========================================================

def build_raw_document(
    row: dict,
    run_id: str,
    source_file: Path,
    source_row_number: int,
) -> dict:
    """
    Build one raw MongoDB document.

    No cleaning or transformation is applied here.
    """

    return {
        "run_id": run_id,
        "source_file": str(source_file.resolve()),
        "source_row_number": source_row_number,
        "ingested_at": datetime.now(timezone.utc),
        "engine_used": "python_batch",
        "raw_record": row,
    }


# =========================================================
# INSERT ONE BATCH
# =========================================================

def insert_batch(
    collection,
    documents: list,
    batch_number: int,
) -> dict:
    """
    Insert one batch into MongoDB and return batch metrics.
    """

    if not documents:
        return {
            "inserted_count": 0,
            "elapsed_seconds": 0.0,
            "throughput": 0.0,
        }

    start_time = time.perf_counter()

    try:
        result = collection.insert_many(
            documents,
            ordered=False,
        )

        inserted_count = len(result.inserted_ids)

    except PyMongoError as error:
        print()
        print(f"BATCH {batch_number}: FAIL")
        print(f"ERROR TYPE: {type(error).__name__}")
        print(f"ERROR: {error}")
        raise

    elapsed_seconds = time.perf_counter() - start_time

    throughput = (
        inserted_count / elapsed_seconds
        if elapsed_seconds > 0
        else 0.0
    )

    return {
        "inserted_count": inserted_count,
        "elapsed_seconds": elapsed_seconds,
        "throughput": throughput,
    }


# =========================================================
# PYTHON BATCH RAW LOADER
# =========================================================

def load_csv_to_raw(
    input_path: Path,
    batch_size: int,
    run_id: str,
) -> dict:
    """
    Stream CSV rows into MongoDB orders_raw.

    The file is never loaded completely into memory.
    """

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    if not input_path.is_file():
        raise ValueError(
            f"Input path is not a file: {input_path}"
        )

    if batch_size <= 0:
        raise ValueError(
            "batch_size must be greater than 0"
        )

    client = None

    total_start = time.perf_counter()

    rows_read = 0
    rows_loaded = 0
    batch_number = 0

    documents = []

    try:
        client = get_mongo_client()

        database = get_database(client)

        raw_collection = get_raw_collection(
            database
        )

        with input_path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as csv_file:

            reader = csv.DictReader(
                csv_file
            )

            if not reader.fieldnames:
                raise ValueError(
                    "CSV header is missing."
                )

            # الصف الأول هو Header،
            # لذلك أول Data Row هو الصف رقم 2.
            for source_row_number, row in enumerate(
                reader,
                start=2,
            ):
                rows_read += 1

                document = build_raw_document(
                    row=row,
                    run_id=run_id,
                    source_file=input_path,
                    source_row_number=source_row_number,
                )

                documents.append(
                    document
                )

                # عند اكتمال حجم Batch
                if len(documents) >= batch_size:

                    batch_number += 1

                    metrics = insert_batch(
                        collection=raw_collection,
                        documents=documents,
                        batch_number=batch_number,
                    )

                    rows_loaded += (
                        metrics["inserted_count"]
                    )

                    print(
                        f"BATCH {batch_number} | "
                        f"rows={metrics['inserted_count']} | "
                        f"seconds={metrics['elapsed_seconds']:.3f} | "
                        f"throughput="
                        f"{metrics['throughput']:.2f} rows/sec"
                    )

                    # تفريغ الذاكرة بعد كل Batch
                    documents.clear()

            # آخر Batch إذا كانت أقل من الحجم المحدد
            if documents:

                batch_number += 1

                metrics = insert_batch(
                    collection=raw_collection,
                    documents=documents,
                    batch_number=batch_number,
                )

                rows_loaded += (
                    metrics["inserted_count"]
                )

                print(
                    f"BATCH {batch_number} | "
                    f"rows={metrics['inserted_count']} | "
                    f"seconds={metrics['elapsed_seconds']:.3f} | "
                    f"throughput="
                    f"{metrics['throughput']:.2f} rows/sec"
                )

                documents.clear()

        total_elapsed = (
            time.perf_counter()
            - total_start
        )

        throughput = (
            rows_loaded / total_elapsed
            if total_elapsed > 0
            else 0.0
        )

        return {
            "run_id": run_id,
            "engine_used": "python_batch",
            "rows_read": rows_read,
            "rows_loaded": rows_loaded,
            "batch_count": batch_number,
            "batch_size": batch_size,
            "elapsed_seconds": total_elapsed,
            "throughput": throughput,
        }

    finally:
        if client is not None:
            client.close()


# =========================================================
# COMMAND LINE ARGUMENTS
# =========================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "Stream a small CSV file into MongoDB orders_raw "
            "using configurable Python batches."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to input CSV file.",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help="Number of records per MongoDB batch.",
    )

    return parser.parse_args()


# =========================================================
# MAIN
# =========================================================

def main():

    args = parse_arguments()

    # معرف فريد لكل Run
    run_id = str(
        uuid.uuid4()
    )

    print(
        "===== PYTHON BATCH RAW LOADER ====="
    )

    print(
        f"Run ID      : {run_id}"
    )

    print(
        f"Input file  : {args.input}"
    )

    print(
        f"Batch size  : {args.batch_size}"
    )

    print(
        "Engine      : python_batch"
    )

    print()

    try:

        result = load_csv_to_raw(
            input_path=args.input,
            batch_size=args.batch_size,
            run_id=run_id,
        )

        print()
        print(
            "===== FINAL RESULT ====="
        )

        print(
            f"Run ID           : "
            f"{result['run_id']}"
        )

        print(
            f"Rows read        : "
            f"{result['rows_read']}"
        )

        print(
            f"Rows loaded raw  : "
            f"{result['rows_loaded']}"
        )

        print(
            f"Batch count      : "
            f"{result['batch_count']}"
        )

        print(
            f"Batch size       : "
            f"{result['batch_size']}"
        )

        print(
            f"Elapsed seconds  : "
            f"{result['elapsed_seconds']:.2f}"
        )

        print(
            f"Throughput       : "
            f"{result['throughput']:.2f} rows/sec"
        )

        if (
            result["rows_read"]
            == result["rows_loaded"]
        ):
            print(
                "RAW LOAD CONSISTENCY: PASS"
            )
        else:
            print(
                "RAW LOAD CONSISTENCY: FAIL"
            )

        print(
            "PYTHON BATCH RAW LOAD: PASS"
        )

    except Exception as error:

        print()

        print(
            "PYTHON BATCH RAW LOAD: FAIL"
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