from pathlib import Path

from src.file_router import route_file


def test_small_file_routes_to_python_batch():
    file_path = Path("data/orders_small_sample.csv")

    result = route_file(file_path)

    assert result["engine"] == "python_batch"
    assert result["file_size_mb"] <= result["threshold_mb"]


def test_large_file_routes_to_pyspark():
    file_path = Path(
        r"C:\Users\Sultan\Desktop\lec5\orders_huge_mixed_quality.csv"
    )

    result = route_file(file_path)

    assert result["engine"] == "pyspark"
    assert result["file_size_mb"] > result["threshold_mb"]