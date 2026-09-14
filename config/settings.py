from pathlib import Path


# ============================================================
# Project Root
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ============================================================
# Data Directories
# ============================================================

DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
SCREENSHOTS_DIR = REPORTS_DIR / "screenshots"


# ============================================================
# Source Data
# ============================================================

DEFAULT_INPUT_FILE = DATA_DIR / "orders_huge_mixed_quality.csv"


# ============================================================
# Small Sample Configuration
# ============================================================

SMALL_SAMPLE_ROWS = 100_000

SMALL_SAMPLE_FILE = DATA_DIR / "orders_sample.csv"


# ============================================================
# File Router Configuration
# ============================================================

# According to the project specification:
# Files <= 200 MB -> Python Batch
# Files > 200 MB  -> PySpark

SMALL_FILE_THRESHOLD_MB = 200


# ============================================================
# Python Batch Configuration
# ============================================================

BATCH_SIZE = 5_000


# ============================================================
# MongoDB Configuration
# ============================================================

MONGO_URI = "mongodb://localhost:27017"

MONGO_DATABASE = "midterm_data_pipeline"

RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"


# ============================================================
# Utility Functions
# ============================================================

def ensure_directories():
    """
    Create required project directories if they do not exist.
    """

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)