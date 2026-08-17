import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RESULTS_FILE = (
    PROJECT_ROOT
    / "reports"
    / "results.json"
)


def load_results():
    with open(
        RESULTS_FILE,
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


def test_big_run_final_counts():

    results = load_results()

    assert results["rows_read"] == 30000000
    assert results["raw_loaded"] == 30000000

    assert results["valid_count"] == 22466172
    assert results["corrected_count"] == 5108915
    assert results["quarantine_count"] == 2424913

    assert (
        results["terminal_output_total"]
        == 30000000
    )


def test_big_run_consistency():

    results = load_results()

    raw_count = results["raw_loaded"]

    output_count = (
        results["valid_count"]
        + results["corrected_count"]
        + results["quarantine_count"]
    )

    assert raw_count == output_count

    assert (
        results[
            "consistency_check"
        ][
            "status"
        ]
        == "PASS"
    )