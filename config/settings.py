from pathlib import Path
import os


# =========================================================
# PROJECT PATHS
# =========================================================

# مجلد المشروع الرئيسي
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# المجلدات الأساسية
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
SCREENSHOTS_DIR = REPORTS_DIR / "screenshots"


# =========================================================
# INPUT FILES
# =========================================================

# ملف البيانات الضخم الافتراضي
INPUT_FILE = Path(
    os.getenv(
        "INPUT_FILE",
        str(DATA_DIR / "orders_huge_mixed_quality.csv"),
    )
)

# العينة الصغيرة التي سننشئها لاحقاً بالسكربت
SMALL_SAMPLE_FILE = Path(
    os.getenv(
        "SMALL_SAMPLE_FILE",
        str(DATA_DIR / "orders_small_sample.csv"),
    )
)


# =========================================================
# FILE ROUTER SETTINGS
# =========================================================

# إذا كان حجم الملف <= هذا الحد نستخدم Python Batch
# وإذا كان أكبر نستخدم PySpark
SMALL_FILE_THRESHOLD_MB = float(
    os.getenv("SMALL_FILE_THRESHOLD_MB", "200")
)


# =========================================================
# PYTHON BATCH SETTINGS
# =========================================================

# عدد السجلات في كل دفعة أثناء insert_many
BATCH_SIZE = int(
    os.getenv("BATCH_SIZE", "5000")
)


# =========================================================
# SMALL SAMPLE SETTINGS
# =========================================================

# عدد صفوف العينة الافتراضي
SAMPLE_ROWS = int(
    os.getenv("SAMPLE_ROWS", "100000")
)


# =========================================================
# MONGODB SETTINGS
# =========================================================

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://127.0.0.1:27017",
)

MONGO_DB_NAME = os.getenv(
    "MONGO_DB_NAME",
    "midterm_data_pipeline",
)

RAW_COLLECTION = os.getenv(
    "RAW_COLLECTION",
    "orders_raw",
)

VALIDATED_COLLECTION = os.getenv(
    "VALIDATED_COLLECTION",
    "orders_validated",
)

QUARANTINE_COLLECTION = os.getenv(
    "QUARANTINE_COLLECTION",
    "orders_quarantine",
)


# =========================================================
# SPARK SETTINGS
# =========================================================

SPARK_MASTER = os.getenv(
    "SPARK_MASTER",
    "local[*]",
)

SPARK_APP_NAME = os.getenv(
    "SPARK_APP_NAME",
    "MidtermDataPipeline",
)


# =========================================================
# REPORTS / LOGGING
# =========================================================

RESULTS_JSON = REPORTS_DIR / "results.json"

RESULTS_MD = REPORTS_DIR / "results.md"

LOG_FILE = REPORTS_DIR / "pipeline.log"