from copy import deepcopy

from src.quality_rules import apply_quality_rules


# =========================================================
# QUALITY CLASSIFICATIONS
# =========================================================

VALID = "Valid"
CORRECTED = "Corrected"
QUARANTINED = "Quarantined"


# =========================================================
# CLASSIFY QUALITY RESULT
# =========================================================

def classify_quality_result(
    quality_result: dict,
) -> dict:
    """
    Classify a record as:

    Valid
    Corrected
    Quarantined

    Priority:
    1. Errors       -> Quarantined
    2. Corrections  -> Corrected
    3. Otherwise    -> Valid
    """

    cleaned_record = deepcopy(
        quality_result.get(
            "cleaned_record",
            {},
        )
    )

    corrections = deepcopy(
        quality_result.get(
            "corrections",
            [],
        )
    )

    errors = deepcopy(
        quality_result.get(
            "errors",
            [],
        )
    )

    # -----------------------------------------------------
    # QUARANTINED
    # -----------------------------------------------------

    if errors:

        reason_codes = []

        for error in errors:

            code = error.get(
                "code"
            )

            if (
                code
                and code not in reason_codes
            ):
                reason_codes.append(
                    code
                )

        return {
            "classification": QUARANTINED,
            "cleaned_record": cleaned_record,
            "corrections": corrections,
            "errors": errors,
            "reason_codes": reason_codes,
        }

    # -----------------------------------------------------
    # CORRECTED
    # -----------------------------------------------------

    if corrections:

        return {
            "classification": CORRECTED,
            "cleaned_record": cleaned_record,
            "corrections": corrections,
            "errors": [],
            "reason_codes": [],
        }

    # -----------------------------------------------------
    # VALID
    # -----------------------------------------------------

    return {
        "classification": VALID,
        "cleaned_record": cleaned_record,
        "corrections": [],
        "errors": [],
        "reason_codes": [],
    }


# =========================================================
# PROCESS ONE RAW RECORD
# =========================================================

def process_record(
    raw_record: dict,
) -> dict:
    """
    Apply quality rules and then classify one record.
    """

    quality_result = apply_quality_rules(
        raw_record
    )

    classification_result = (
        classify_quality_result(
            quality_result
        )
    )

    return classification_result