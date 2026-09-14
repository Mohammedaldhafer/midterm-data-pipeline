import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from quality_rules import (
    normalize_phone,
    normalize_email,
    normalize_date,
    normalize_whitespace,
    normalize_numeric_value,
    apply_quality_rules,
)


def test_phone_normalization():
    assert normalize_phone("967+ 77 123 4567") == "967+771234567"


def test_email_normalization():
    assert normalize_email("user@@mail..com") == "user@mail.com"


def test_date_normalization():
    assert normalize_date("2025/01/31") == "2025-01-31"


def test_whitespace_normalization():
    assert normalize_whitespace("  Hello   World  ") == "Hello World"


def test_numeric_normalization():
    assert normalize_numeric_value("1,500") == "1500"


def test_quality_record_contains_corrections():
    record = {
        "id_order": "1001",
        "customer_phone": "967+ 77 123 4567",
        "customer_email": "user@@mail..com",
        "order_date": "2025/01/31",
    }

    result = apply_quality_rules(record)

    assert result["quality_status"] == "corrected"
    assert len(result["corrections"]) > 0