from pathlib import Path

from src.file_router import route_file


# =========================================================
# PROJECT PATHS
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SMALL_SAMPLE = (
    PROJECT_ROOT
    / "data"
    / "orders_small_sample.csv"
)


# =========================================================
# TEST SMALL FILE
# =========================================================

def test_small_file_routes_to_python_batch():
    """
    التأكد أن الملف الصغير يذهب إلى Python Batch.
    """

    result = route_file(
        SMALL_SAMPLE
    )

    assert result["engine"] == "python_batch"

    assert (
        result["file_size_mb"]
        <= result["threshold_mb"]
    )


# =========================================================
# TEST LARGE FILE
# =========================================================

def test_large_file_routes_to_pyspark(
    monkeypatch,
):
    """
    اختبار اختيار PySpark بدون الحاجة إلى ملف 12GB.

    نستخدم نفس العينة الصغيرة،
    لكن أثناء الاختبار فقط نجعل حجمها الظاهري
    أكبر من الحد المحدد.
    """

    # نأخذ إعداد Threshold الحقيقي
    normal_result = route_file(
        SMALL_SAMPLE
    )

    threshold_mb = normal_result[
        "threshold_mb"
    ]

    # حفظ دالة stat الأصلية
    original_stat = Path.stat

    # الحجم الوهمي:
    # أكبر من Threshold بمقدار 1 MB
    fake_size_bytes = int(
        (threshold_mb + 1)
        * 1024
        * 1024
    )

    class FakeStat:
        """
        يحتفظ بجميع معلومات الملف الأصلية
        ويغير st_size فقط.
        """

        def __init__(
            self,
            real_stat,
        ):
            self._real_stat = real_stat
            self.st_size = fake_size_bytes

        def __getattr__(
            self,
            name,
        ):
            return getattr(
                self._real_stat,
                name,
            )

    def fake_stat(
        self,
        *args,
        **kwargs,
    ):
        real_stat = original_stat(
            self,
            *args,
            **kwargs,
        )

        if self == SMALL_SAMPLE:
            return FakeStat(
                real_stat
            )

        return real_stat

    # تغيير الحجم أثناء هذا الاختبار فقط
    monkeypatch.setattr(
        Path,
        "stat",
        fake_stat,
    )

    result = route_file(
        SMALL_SAMPLE
    )

    assert result["engine"] == "pyspark"

    assert (
        result["file_size_mb"]
        > result["threshold_mb"]
    )