import argparse
import sys
import time
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from pymongo import UpdateOne


# =========================================================
# PROJECT IMPORTS
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import BATCH_SIZE

from src.classification import (
    VALID,
    CORRECTED,
    QUARANTINED,
    process_record,
)

from src.mongo_setup import (
    get_mongo_client,
    get_database,
    get_raw_collection,
    get_validated_collection,
    get_quarantine_collection,
)


# =========================================================
# BUILD VALIDATED DOCUMENT
# =========================================================

def build_validated_document(
    raw_document: dict,
    classification_result: dict,
) -> dict:
    """
    Build one document for orders_validated.
    """

    classification = classification_result[
        "classification"
    ]

    if classification not in [
        VALID,
        CORRECTED,
    ]:
        raise ValueError(
            "Quarantined record cannot be written "
            "to orders_validated."
        )

    cleaned_record = deepcopy(
        classification_result[
            "cleaned_record"
        ]
    )

    order_id = cleaned_record.get(
        "order_id"
    )

    customer_id = cleaned_record.get(
        "customer_id"
    )

    if (
        order_id is None
        or str(order_id).strip() == ""
    ):
        raise ValueError(
            "Validated document requires order_id."
        )

    if (
        customer_id is None
        or str(customer_id).strip() == ""
    ):
        raise ValueError(
            "Validated document requires customer_id."
        )

    document = deepcopy(
        cleaned_record
    )

    document.update(
        {
            "quality_status": classification,

            "corrections": deepcopy(
                classification_result.get(
                    "corrections",
                    [],
                )
            ),

            "run_id": raw_document.get(
                "run_id"
            ),

            "source_file": raw_document.get(
                "source_file"
            ),

            "source_row_number": raw_document.get(
                "source_row_number"
            ),

            "engine_used": raw_document.get(
                "engine_used"
            ),

            "source_ingested_at": raw_document.get(
                "ingested_at"
            ),
        }
    )

    return document


# =========================================================
# SINGLE RECORD UPSERT
# =========================================================

def upsert_validated_record(
    collection,
    validated_document: dict,
) -> dict:
    """
    Upsert one validated business record by order_id.

    Used also by idempotency tests.
    """

    order_id = validated_document.get(
        "order_id"
    )

    if (
        order_id is None
        or str(order_id).strip() == ""
    ):
        raise ValueError(
            "order_id is required for Upsert."
        )

    result = collection.update_one(
        {
            "order_id": order_id,
        },
        {
            "$set": validated_document,

            "$setOnInsert": {
                "validated_at": datetime.now(
                    timezone.utc
                ),
            },
        },
        upsert=True,
    )

    if result.upserted_id is not None:

        return {
            "status": "inserted",
            "inserted": 1,
            "updated": 0,
            "unchanged": 0,
        }

    if result.modified_count > 0:

        return {
            "status": "updated",
            "inserted": 0,
            "updated": 1,
            "unchanged": 0,
        }

    return {
        "status": "unchanged",
        "inserted": 0,
        "updated": 0,
        "unchanged": 1,
    }


# =========================================================
# BUILD QUARANTINE DOCUMENT
# =========================================================

def build_quarantine_document(
    raw_document: dict,
    classification_result: dict,
) -> dict:
    """
    Build one diagnostic quarantine document.
    """

    if (
        classification_result[
            "classification"
        ]
        != QUARANTINED
    ):
        raise ValueError(
            "Only Quarantined records can be "
            "written to orders_quarantine."
        )

    return {
        "run_id": raw_document.get(
            "run_id"
        ),

        "source_file": raw_document.get(
            "source_file"
        ),

        "source_row_number": raw_document.get(
            "source_row_number"
        ),

        "engine_used": raw_document.get(
            "engine_used"
        ),

        "source_ingested_at": raw_document.get(
            "ingested_at"
        ),

        "quarantined_at": datetime.now(
            timezone.utc
        ),

        # Raw stays preserved.
        "raw_record": deepcopy(
            raw_document.get(
                "raw_record",
                {},
            )
        ),

        "cleaned_record": deepcopy(
            classification_result.get(
                "cleaned_record",
                {},
            )
        ),

        "corrections": deepcopy(
            classification_result.get(
                "corrections",
                [],
            )
        ),

        "errors": deepcopy(
            classification_result.get(
                "errors",
                [],
            )
        ),

        "reason_codes": deepcopy(
            classification_result.get(
                "reason_codes",
                [],
            )
        ),
    }


# =========================================================
# SINGLE QUARANTINE INSERT
# =========================================================

def insert_quarantine_record(
    collection,
    quarantine_document: dict,
):
    """
    Insert one quarantine document.
    """

    result = collection.insert_one(
        quarantine_document
    )

    return result.inserted_id


# =========================================================
# DUPLICATE ORDER POLICY
# =========================================================

def mark_as_duplicate(
    classification_result: dict,
    order_id: str,
) -> dict:
    """
    Later occurrences of the same order_id in the
    same Raw run are quarantined.

    Policy:
    First occurrence wins.
    Later occurrences -> DUPLICATE_ORDER_ID.
    """

    result = deepcopy(
        classification_result
    )

    result[
        "classification"
    ] = QUARANTINED

    duplicate_error = {
        "code": "DUPLICATE_ORDER_ID",
        "field": "order_id",
        "value": order_id,
        "message": (
            "Duplicate order_id was found "
            "inside the same processing run."
        ),
    }

    result.setdefault(
        "errors",
        [],
    ).append(
        duplicate_error
    )

    reason_codes = result.setdefault(
        "reason_codes",
        [],
    )

    if (
        "DUPLICATE_ORDER_ID"
        not in reason_codes
    ):
        reason_codes.append(
            "DUPLICATE_ORDER_ID"
        )

    return result


# =========================================================
# BULK WRITE
# =========================================================

def flush_writes(
    validated_collection,
    quarantine_collection,
    validated_operations: list,
    quarantine_documents: list,
) -> dict:
    """
    Write one processing batch to MongoDB.

    Valid/Corrected -> Bulk Upsert
    Quarantined     -> insert_many
    """

    inserted = 0
    updated = 0
    unchanged = 0
    quarantine_written = 0

    # -----------------------------------------------------
    # VALIDATED BULK UPSERT
    # -----------------------------------------------------

    if validated_operations:

        result = validated_collection.bulk_write(
            validated_operations,
            ordered=False,
        )

        inserted = result.upserted_count

        updated = result.modified_count

        unchanged = (
            result.matched_count
            - result.modified_count
        )

    # -----------------------------------------------------
    # QUARANTINE BULK INSERT
    # -----------------------------------------------------

    if quarantine_documents:

        result = (
            quarantine_collection.insert_many(
                quarantine_documents,
                ordered=False,
            )
        )

        quarantine_written = len(
            result.inserted_ids
        )

    return {
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "quarantine_written": quarantine_written,
    }


# =========================================================
# END-TO-END PYTHON ELT
# =========================================================

def process_raw_run(
    run_id: str,
    progress_every: int = BATCH_SIZE,
) -> dict:
    """
    Process one existing Raw run.

    Designed for the Python Batch / small-file path.

    It streams MongoDB Raw documents and does not load
    the entire Raw run into a Python list.
    """

    if not run_id:
        raise ValueError(
            "run_id is required."
        )

    if progress_every <= 0:
        raise ValueError(
            "progress_every must be greater than 0."
        )

    client = None
    cursor = None

    start_time = time.perf_counter()

    # Business IDs only, not entire documents.
    # Safe for the small Python path.
    seen_order_ids = set()

    classification_counts = {
        VALID: 0,
        CORRECTED: 0,
        QUARANTINED: 0,
    }

    reason_counts = Counter()

    inserted = 0
    updated = 0
    unchanged = 0
    quarantine_written = 0

    processed = 0

    validated_operations = []
    quarantine_documents = []

    try:

        client = get_mongo_client()

        database = get_database(
            client
        )

        raw_collection = get_raw_collection(
            database
        )

        validated_collection = (
            get_validated_collection(
                database
            )
        )

        quarantine_collection = (
            get_quarantine_collection(
                database
            )
        )

        # -------------------------------------------------
        # RAW COUNT
        # -------------------------------------------------

        raw_count = raw_collection.count_documents(
            {
                "run_id": run_id
            }
        )

        if raw_count == 0:

            raise ValueError(
                f"No Raw records found for run_id: {run_id}"
            )

        # Protect against accidental duplicate quarantine
        # writes from processing the same run twice.
        existing_quarantine = (
            quarantine_collection.find_one(
                {
                    "run_id": run_id
                },
                {
                    "_id": 1
                },
            )
        )

        if existing_quarantine is not None:

            raise RuntimeError(
                "This run already has quarantine output. "
                "Do not process it again without cleanup."
            )

        print(
            "===== END-TO-END ELT ====="
        )

        print(
            f"Run ID          : {run_id}"
        )

        print(
            f"Raw count       : {raw_count}"
        )

        print(
            f"Progress batch  : {progress_every}"
        )

        print()

        # -------------------------------------------------
        # STREAM RAW FROM MONGODB
        # -------------------------------------------------

        cursor = raw_collection.find(
            {
                "run_id": run_id
            },
            {
                "_id": 0,
                "run_id": 1,
                "source_file": 1,
                "source_row_number": 1,
                "ingested_at": 1,
                "engine_used": 1,
                "raw_record": 1,
            },
            no_cursor_timeout=True,
        ).batch_size(
            progress_every
        )

        for raw_document in cursor:

            processed += 1

            raw_record = raw_document.get(
                "raw_record",
                {},
            )

            # ---------------------------------------------
            # QUALITY + CLASSIFICATION
            # ---------------------------------------------

            result = process_record(
                raw_record
            )

            cleaned_record = result.get(
                "cleaned_record",
                {},
            )

            order_id = cleaned_record.get(
                "order_id"
            )

            normalized_order_id = None

            if order_id is not None:

                normalized_order_id = (
                    str(order_id).strip()
                )

            # ---------------------------------------------
            # DUPLICATE INSIDE SAME RUN
            # ---------------------------------------------

            if normalized_order_id:

                if (
                    normalized_order_id
                    in seen_order_ids
                ):

                    result = mark_as_duplicate(
                        result,
                        normalized_order_id,
                    )

                else:

                    seen_order_ids.add(
                        normalized_order_id
                    )

            classification = result[
                "classification"
            ]

            classification_counts[
                classification
            ] += 1

            # ---------------------------------------------
            # QUARANTINE
            # ---------------------------------------------

            if classification == QUARANTINED:

                quarantine_document = (
                    build_quarantine_document(
                        raw_document,
                        result,
                    )
                )

                quarantine_documents.append(
                    quarantine_document
                )

                for code in result.get(
                    "reason_codes",
                    [],
                ):

                    reason_counts[
                        code
                    ] += 1

            # ---------------------------------------------
            # VALID / CORRECTED
            # ---------------------------------------------

            else:

                validated_document = (
                    build_validated_document(
                        raw_document,
                        result,
                    )
                )

                business_key = (
                    validated_document[
                        "order_id"
                    ]
                )

                operation = UpdateOne(
                    {
                        "order_id": business_key
                    },
                    {
                        "$set": (
                            validated_document
                        ),

                        "$setOnInsert": {
                            "validated_at": (
                                datetime.now(
                                    timezone.utc
                                )
                            )
                        },
                    },
                    upsert=True,
                )

                validated_operations.append(
                    operation
                )

            # ---------------------------------------------
            # FLUSH BATCH
            # ---------------------------------------------

            if (
                processed % progress_every
                == 0
            ):

                write_result = flush_writes(
                    validated_collection,
                    quarantine_collection,
                    validated_operations,
                    quarantine_documents,
                )

                inserted += (
                    write_result[
                        "inserted"
                    ]
                )

                updated += (
                    write_result[
                        "updated"
                    ]
                )

                unchanged += (
                    write_result[
                        "unchanged"
                    ]
                )

                quarantine_written += (
                    write_result[
                        "quarantine_written"
                    ]
                )

                validated_operations.clear()

                quarantine_documents.clear()

                elapsed = (
                    time.perf_counter()
                    - start_time
                )

                throughput = (
                    processed / elapsed
                    if elapsed > 0
                    else 0.0
                )

                print(
                    f"Processed={processed}/{raw_count} | "
                    f"Valid={classification_counts[VALID]} | "
                    f"Corrected={classification_counts[CORRECTED]} | "
                    f"Quarantined={classification_counts[QUARANTINED]} | "
                    f"Rate={throughput:.2f} rows/sec"
                )

        # -------------------------------------------------
        # FINAL PARTIAL BATCH
        # -------------------------------------------------

        if (
            validated_operations
            or quarantine_documents
        ):

            write_result = flush_writes(
                validated_collection,
                quarantine_collection,
                validated_operations,
                quarantine_documents,
            )

            inserted += (
                write_result[
                    "inserted"
                ]
            )

            updated += (
                write_result[
                    "updated"
                ]
            )

            unchanged += (
                write_result[
                    "unchanged"
                ]
            )

            quarantine_written += (
                write_result[
                    "quarantine_written"
                ]
            )

            validated_operations.clear()

            quarantine_documents.clear()

        # -------------------------------------------------
        # FINAL METRICS
        # -------------------------------------------------

        elapsed_seconds = (
            time.perf_counter()
            - start_time
        )

        throughput = (
            processed / elapsed_seconds
            if elapsed_seconds > 0
            else 0.0
        )

        terminal_count = (
            classification_counts[
                VALID
            ]
            + classification_counts[
                CORRECTED
            ]
            + classification_counts[
                QUARANTINED
            ]
        )

        consistency_pass = (
            processed
            == raw_count
            == terminal_count
        )

        return {
            "run_id": run_id,
            "raw_count": raw_count,
            "processed": processed,

            "valid_count": (
                classification_counts[
                    VALID
                ]
            ),

            "corrected_count": (
                classification_counts[
                    CORRECTED
                ]
            ),

            "quarantine_count": (
                classification_counts[
                    QUARANTINED
                ]
            ),

            "quarantine_written": (
                quarantine_written
            ),

            "inserted_count": inserted,
            "updated_count": updated,
            "unchanged_count": unchanged,

            "reason_counts": dict(
                reason_counts
            ),

            "elapsed_seconds": (
                elapsed_seconds
            ),

            "throughput": throughput,

            "consistency_pass": (
                consistency_pass
            ),
        }

    finally:

        if cursor is not None:

            try:
                cursor.close()
            except Exception:
                pass

        if client is not None:
            client.close()


# =========================================================
# CLI
# =========================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "Process one Raw MongoDB run through "
            "quality rules, classification, Upsert "
            "and quarantine."
        )
    )

    parser.add_argument(
        "--run-id",
        required=True,
        help="Raw run_id to process.",
    )

    parser.add_argument(
        "--progress-every",
        type=int,
        default=BATCH_SIZE,
        help=(
            "Number of Raw records processed "
            "before each bulk write."
        ),
    )

    return parser.parse_args()


# =========================================================
# MAIN
# =========================================================

def main():

    args = parse_arguments()

    try:

        result = process_raw_run(
            run_id=args.run_id,
            progress_every=args.progress_every,
        )

        print()
        print(
            "===== ELT FINAL RESULT ====="
        )

        print(
            f"Run ID             : "
            f"{result['run_id']}"
        )

        print(
            f"Raw count          : "
            f"{result['raw_count']}"
        )

        print(
            f"Processed          : "
            f"{result['processed']}"
        )

        print(
            f"Valid              : "
            f"{result['valid_count']}"
        )

        print(
            f"Corrected          : "
            f"{result['corrected_count']}"
        )

        print(
            f"Quarantined        : "
            f"{result['quarantine_count']}"
        )

        print(
            f"Quarantine written : "
            f"{result['quarantine_written']}"
        )

        print(
            f"Inserted           : "
            f"{result['inserted_count']}"
        )

        print(
            f"Updated            : "
            f"{result['updated_count']}"
        )

        print(
            f"Unchanged          : "
            f"{result['unchanged_count']}"
        )

        print(
            f"Elapsed seconds    : "
            f"{result['elapsed_seconds']:.2f}"
        )

        print(
            f"Throughput         : "
            f"{result['throughput']:.2f} rows/sec"
        )

        print()
        print(
            "===== ERROR CASE COUNTS ====="
        )

        if result["reason_counts"]:

            for (
                reason,
                count,
            ) in sorted(
                result[
                    "reason_counts"
                ].items()
            ):

                print(
                    f"{reason}: {count}"
                )

        else:

            print(
                "No quarantine reasons."
            )

        print()

        print(
            "CONSISTENCY:",
            (
                "PASS"
                if result[
                    "consistency_pass"
                ]
                else "FAIL"
            ),
        )

        if not result[
            "consistency_pass"
        ]:

            raise RuntimeError(
                "ELT consistency check failed."
            )

        print(
            "END-TO-END ELT: PASS"
        )

    except Exception as error:

        print()
        print(
            "END-TO-END ELT: FAIL"
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