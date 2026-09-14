from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REQUIRED_METRIC_FIELDS = [
    "id_run",
    "file_name",
    "file_size_mb",
    "used_engine",
    "read_rows",
    "loaded_raw",
    "count_valid",
    "count_corrected",
    "count_quarantine",
    "seconds_elapsed",
    "throughput",
    "partitions",
    "size_batch",
    "counts_case_error",
    "count_inserted",
    "count_updated",
    "count_unchanged",
]


def build_metrics(
    *,
    id_run: str,
    file_name: str,
    file_size_mb: float,
    used_engine: str,
    read_rows: int,
    loaded_raw: int,
    count_valid: int,
    count_corrected: int,
    count_quarantine: int,
    seconds_elapsed: float,
    partitions: int | None = None,
    size_batch: int | None = None,
    counts_case_error: dict[str, int] | None = None,
    count_inserted: int = 0,
    count_updated: int = 0,
    count_unchanged: int = 0,
) -> dict[str, Any]:
    """Build a standardized metrics dictionary for one pipeline run."""

    seconds_elapsed = float(seconds_elapsed)
    throughput = (
        read_rows / seconds_elapsed
        if seconds_elapsed > 0
        else 0.0
    )

    return {
        "id_run": id_run,
        "file_name": file_name,
        "file_size_mb": float(file_size_mb),
        "used_engine": used_engine,
        "read_rows": int(read_rows),
        "loaded_raw": int(loaded_raw),
        "count_valid": int(count_valid),
        "count_corrected": int(count_corrected),
        "count_quarantine": int(count_quarantine),
        "seconds_elapsed": seconds_elapsed,
        "throughput": round(throughput, 2),
        "partitions": partitions,
        "size_batch": size_batch,
        "counts_case_error": counts_case_error or {},
        "count_inserted": int(count_inserted),
        "count_updated": int(count_updated),
        "count_unchanged": int(count_unchanged),
    }


def validate_metrics(metrics: dict[str, Any]) -> bool:
    """Validate that all required assignment metrics are present."""

    missing = [
        field for field in REQUIRED_METRIC_FIELDS
        if field not in metrics
    ]

    if missing:
        raise ValueError(
            f"Missing required metrics fields: {', '.join(missing)}"
        )

    return True


def save_metrics(metrics: dict[str, Any], output_file: str | Path) -> Path:
    """Validate and save metrics as JSON."""

    validate_metrics(metrics)

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    output_path.write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return output_path