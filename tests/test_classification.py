import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from file_router import select_engine


def test_small_file_uses_python_batch():
    path = PROJECT_ROOT / "data" / "test_router_small.tmp"
    path.write_bytes(b"x")

    try:
        result = select_engine(path)

        assert result["engine"] == "python_batch"
        assert result["file_size_mb"] <= 200
    finally:
        path.unlink(missing_ok=True)


def test_nonempty_file_has_valid_router_decision():
    path = PROJECT_ROOT / "data" / "test_router_large.tmp"
    path.write_bytes(b"x")

    try:
        result = select_engine(path)

        assert result["engine"] in {
            "python_batch",
            "pyspark"
        }
        assert result["file_size_mb"] >= 0
        assert result["threshold_mb"] == 200
        assert result["reason"]
    finally:
        path.unlink(missing_ok=True)