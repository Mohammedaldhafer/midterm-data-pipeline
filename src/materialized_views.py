from __future__ import annotations

import argparse
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from pymongo import MongoClient


MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "midterm_data_pipeline"

VALIDATED_COLLECTION = "orders_validated"
DAILY_SALES_COLLECTION = "daily_sales_summary"
TOP_PRODUCTS_COLLECTION = "top_products_summary"
MV_STATE_COLLECTION = "mv_order_state"


def get_database():
    """Connect to MongoDB and return the project database."""

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )

    client.admin.command("ping")
    return client, client[DATABASE_NAME]


def parse_amount(value: Any) -> float:
    """Convert common string/numeric amount formats to float."""

    if value is None:
        return 0.0

    text = str(value).strip()

    if not text:
        return 0.0

    text = (
        text
        .replace(",", "")
        .replace("٫", ".")
    )

    try:
        return float(Decimal(text))
    except (InvalidOperation, ValueError):
        return 0.0


def parse_date(value: Any) -> datetime | None:
    """Convert an order date into a Python datetime."""

    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    text = str(value).strip()

    if not text:
        return None

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def sales_day(document: dict[str, Any]) -> str | None:
    """Return YYYY-MM-DD for an order document."""

    parsed = parse_date(document.get("order_date"))

    if parsed is None:
        return None

    return parsed.strftime("%Y-%m-%d")


def parse_items(value: Any) -> list[dict[str, Any]]:
    """
    Safely parse items_json.

    The current project stores items_json as a JSON string.
    This function also accepts an already-parsed list.
    """

    if isinstance(value, list):
        return [
            item
            for item in value
            if isinstance(item, dict)
        ]

    if not isinstance(value, str):
        return []

    try:
        parsed = json.loads(value)

        if not isinstance(parsed, list):
            return []

        return [
            item
            for item in parsed
            if isinstance(item, dict)
        ]

    except (json.JSONDecodeError, TypeError):
        return []


def item_contributions(document: dict[str, Any]):
    """
    Return aggregated product contributions for one order.

    Each contribution is:
    {
        "sku": ...,
        "product_name": ...,
        "quantity_sold": ...,
        "sales": ...
    }
    """

    contributions: dict[tuple[str, str], dict[str, float]] = {}

    for item in parse_items(document.get("items_json")):
        sku = str(item.get("sku", "")).strip()
        product_name = str(item.get("name", "")).strip()

        if not sku and not product_name:
            continue

        key = (sku, product_name)

        if key not in contributions:
            contributions[key] = {
                "quantity_sold": 0.0,
                "sales": 0.0,
            }

        contributions[key]["quantity_sold"] += parse_amount(
            item.get("qty", 0)
        )

        contributions[key]["sales"] += parse_amount(
            item.get("total", 0)
        )

    return contributions


def create_mv_indexes(db):
    """Create indexes needed by the materialized-view collections."""

    db[DAILY_SALES_COLLECTION].create_index(
        [("day", 1)],
        unique=True,
        name="unique_day",
    )

    db[TOP_PRODUCTS_COLLECTION].create_index(
        [
            ("sku", 1),
            ("product_name", 1),
        ],
        unique=True,
        name="unique_product",
    )

    db[MV_STATE_COLLECTION].create_index(
        [("id_order", 1)],
        unique=True,
        name="unique_mv_order",
    )


def initialize_daily_sales_summary(db):
    """
    Build daily_sales_summary from orders_validated.

    This is the one-time initial materialization.
    Future updates use apply_incremental_change().
    """

    pipeline = [
        {
            "$set": {
                "_order_date": {
                    "$convert": {
                        "input": "$order_date",
                        "to": "date",
                        "onError": None,
                        "onNull": None,
                    }
                },
                "_amount_text": {
                    "$replaceAll": {
                        "input": {
                            "$ifNull": ["$total_amount", "0"]
                        },
                        "find": ",",
                        "replacement": "",
                    }
                },
            }
        },
        {
            "$set": {
                "_amount_text": {
                    "$replaceAll": {
                        "input": "$_amount_text",
                        "find": "٫",
                        "replacement": ".",
                    }
                }
            }
        },
        {
            "$match": {
                "_order_date": {
                    "$ne": None,
                }
            }
        },
        {
            "$set": {
                "_amount": {
                    "$convert": {
                        "input": "$_amount_text",
                        "to": "double",
                        "onError": 0,
                        "onNull": 0,
                    }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$_order_date",
                    }
                },
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": "$_amount"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "day": "$_id",
                "order_count": 1,
                "total_sales": {
                    "$round": ["$total_sales", 2],
                },
            }
        },
        {
            "$merge": {
                "into": DAILY_SALES_COLLECTION,
                "on": "day",
                "whenMatched": "replace",
                "whenNotMatched": "insert",
            }
        },
    ]

    db[VALIDATED_COLLECTION].aggregate(
        pipeline,
        allowDiskUse=True,
    )

    create_mv_indexes(db)


def initialize_top_products_summary(db):
    """
    Build top_products_summary from orders_validated.

    items_json is parsed safely as JSON and grouped by SKU/name.
    Future changes use apply_incremental_change().
    """

    pipeline = [
        {
            "$set": {
                "_items": {
                    "$function": {
                        "body": """
                            function(value) {
                                if (Array.isArray(value)) {
                                    return value;
                                }

                                if (typeof value === "string") {
                                    try {
                                        const parsed = JSON.parse(value);
                                        return Array.isArray(parsed)
                                            ? parsed
                                            : [];
                                    } catch (e) {
                                        return [];
                                    }
                                }

                                return [];
                            }
                        """,
                        "args": ["$items_json"],
                        "lang": "js",
                    }
                }
            }
        },
        {
            "$unwind": "$_items",
        },
        {
            "$set": {
                "_qty": {
                    "$convert": {
                        "input": {
                            "$ifNull": ["$_items.qty", 0]
                        },
                        "to": "double",
                        "onError": 0,
                        "onNull": 0,
                    }
                },
                "_sales": {
                    "$convert": {
                        "input": {
                            "$ifNull": ["$_items.total", 0]
                        },
                        "to": "double",
                        "onError": 0,
                        "onNull": 0,
                    }
                },
                "_sku": {
                    "$trim": {
                        "input": {
                            "$toString": {
                                "$ifNull": ["$_items.sku", ""]
                            }
                        }
                    }
                },
                "_product_name": {
                    "$trim": {
                        "input": {
                            "$toString": {
                                "$ifNull": ["$_items.name", ""]
                            }
                        }
                    }
                },
            }
        },
        {
            "$match": {
                "_sku": {"$ne": ""},
            }
        },
        {
            "$group": {
                "_id": {
                    "sku": "$_sku",
                    "product_name": "$_product_name",
                },
                "quantity_sold": {"$sum": "$_qty"},
                "sales": {"$sum": "$_sales"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "sku": "$_id.sku",
                "product_name": "$_id.product_name",
                "quantity_sold": {
                    "$round": ["$quantity_sold", 2],
                },
                "sales": {
                    "$round": ["$sales", 2],
                },
            }
        },
        {
            "$merge": {
                "into": TOP_PRODUCTS_COLLECTION,
                "on": [
                    "sku",
                    "product_name",
                ],
                "whenMatched": "replace",
                "whenNotMatched": "insert",
            }
        },
    ]

    db[VALIDATED_COLLECTION].aggregate(
        pipeline,
        allowDiskUse=True,
    )

    create_mv_indexes(db)


def _adjust_daily_summary(
    collection,
    document: dict[str, Any] | None,
    order_delta: int,
    sales_delta: float,
):
    """Apply one order's contribution change to daily_sales_summary."""

    if not document:
        return

    day = sales_day(document)

    if not day:
        return

    collection.update_one(
        {"day": day},
        {
            "$inc": {
                "order_count": order_delta,
                "total_sales": sales_delta,
            }
        },
        upsert=True,
    )

    collection.update_one(
        {"day": day},
        [
            {
                "$set": {
                    "total_sales": {
                        "$round": ["$total_sales", 2]
                    }
                }
            }
        ],
    )


def _adjust_product_summary(
    collection,
    document: dict[str, Any] | None,
    multiplier: int,
):
    """Apply an order's product contribution changes."""

    if not document:
        return

    contributions = item_contributions(document)

    for (sku, product_name), values in contributions.items():
        collection.update_one(
            {
                "sku": sku,
                "product_name": product_name,
            },
            {
                "$inc": {
                    "quantity_sold": (
                        multiplier * values["quantity_sold"]
                    ),
                    "sales": (
                        multiplier * values["sales"]
                    ),
                }
            },
            upsert=(multiplier > 0),
        )

        collection.update_one(
            {
                "sku": sku,
                "product_name": product_name,
            },
            [
                {
                    "$set": {
                        "quantity_sold": {
                            "$round": ["$quantity_sold", 2]
                        },
                        "sales": {
                            "$round": ["$sales", 2]
                        },
                    }
                }
            ],
        )

        collection.delete_one(
            {
                "sku": sku,
                "product_name": product_name,
                "quantity_sold": {"$lte": 0},
                "sales": {"$lte": 0},
            }
        )


def apply_incremental_change(
    db,
    old_document: dict[str, Any] | None,
    new_document: dict[str, Any],
) -> dict[str, Any]:
    """
    Apply one accepted Insert/Update incrementally to both materialized views.

    The caller should invoke this ONLY after the incremental loader has
    accepted the record according to its version rules.

    A small MV state collection additionally protects against accidental
    replay of the same version at the materialized-view layer.
    """

    id_order = str(new_document.get("id_order", "")).strip()

    if not id_order:
        raise ValueError("new_document must contain id_order.")

    new_version = new_document.get("version")

    state = db[MV_STATE_COLLECTION].find_one(
        {"id_order": id_order},
        {
            "_id": 0,
            "version": 1,
        },
    )

    if state and new_version is not None:
        try:
            if int(new_version) <= int(state["version"]):
                return {
                    "applied": False,
                    "reason": "version_already_applied",
                }
        except (TypeError, ValueError):
            pass

    old_amount = parse_amount(
        old_document.get("total_amount")
        if old_document
        else 0
    )

    new_amount = parse_amount(
        new_document.get("total_amount")
    )

    if old_document is not None:
        _adjust_daily_summary(
            db[DAILY_SALES_COLLECTION],
            old_document,
            -1,
            -old_amount,
        )

        _adjust_product_summary(
            db[TOP_PRODUCTS_COLLECTION],
            old_document,
            -1,
        )

    _adjust_daily_summary(
        db[DAILY_SALES_COLLECTION],
        new_document,
        1,
        new_amount,
    )

    _adjust_product_summary(
        db[TOP_PRODUCTS_COLLECTION],
        new_document,
        1,
    )

    state_document = {
        "id_order": id_order,
        "updated_at": datetime.utcnow(),
    }

    if new_version is not None:
        state_document["version"] = str(new_version)

    db[MV_STATE_COLLECTION].update_one(
        {"id_order": id_order},
        {
            "$set": state_document,
        },
        upsert=True,
    )

    return {
        "applied": True,
        "id_order": id_order,
        "operation": "update" if old_document else "insert",
    }


def initialize_all(db):
    """Initialize both materialized-view collections once."""

    db[DAILY_SALES_COLLECTION].drop()
    db[TOP_PRODUCTS_COLLECTION].drop()

    create_mv_indexes(db)

    initialize_daily_sales_summary(db)
    initialize_top_products_summary(db)

    return {
        "daily_sales_summary": db[
            DAILY_SALES_COLLECTION
        ].count_documents({}),
        "top_products_summary": db[
            TOP_PRODUCTS_COLLECTION
        ].count_documents({}),
    }


def show_views(db, limit: int = 10):
    """Display a small preview of both materialized views."""

    print("=" * 80)
    print("daily_sales_summary")
    print("=" * 80)

    for row in db[DAILY_SALES_COLLECTION].find(
        {},
        {"_id": 0},
    ).sort(
        "day",
        1,
    ).limit(limit):
        print(row)

    print()
    print("=" * 80)
    print("top_products_summary")
    print("=" * 80)

    for row in db[TOP_PRODUCTS_COLLECTION].find(
        {},
        {"_id": 0},
    ).sort(
        "sales",
        -1,
    ).limit(limit):
        print(row)


def main():
    parser = argparse.ArgumentParser(
        description="Materialized views for the final Big Data project."
    )

    parser.add_argument(
        "--init",
        action="store_true",
        help="Build both materialized views from orders_validated.",
    )

    parser.add_argument(
        "--show",
        action="store_true",
        help="Show materialized-view previews.",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Preview row limit.",
    )

    args = parser.parse_args()

    client, db = get_database()

    try:
        if args.init:
            result = initialize_all(db)

            print("=" * 80)
            print("MATERIALIZED VIEWS INITIALIZED")
            print("=" * 80)
            print(
                f"daily_sales_summary  : "
                f"{result['daily_sales_summary']:,}"
            )
            print(
                f"top_products_summary : "
                f"{result['top_products_summary']:,}"
            )

        if args.show:
            show_views(
                db,
                limit=args.limit,
            )

        if not args.init and not args.show:
            parser.print_help()

    finally:
        client.close()


if __name__ == "__main__":
    main()
