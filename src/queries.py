from __future__ import annotations

import argparse
from datetime import datetime
from typing import Any

from pymongo import MongoClient


MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "midterm_data_pipeline"
COLLECTION_NAME = "orders_validated"


# These are the indexes we will create in the next step.
# They are deliberately NOT created here because we first need
# executionStats before and after the indexes.
QUERY_INDEXES = {
    "idx_city_order_date": [
        ("city", 1),
        ("order_date", 1),
    ],
    "idx_customer_order_date": [
        ("customer_id", 1),
        ("order_date", 1),
    ],
    "idx_status_city_order_date": [
        ("status", 1),
        ("city", 1),
        ("order_date", 1),
    ],
}


def get_collection():
    """Return the orders_validated collection."""

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )

    client.admin.command("ping")

    db = client[DATABASE_NAME]
    return client, db[COLLECTION_NAME]


def query_recent_orders_by_city(
    collection,
    city: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Return recent orders for a specific city."""

    cursor = (
        collection.find(
            {"city": city},
            {
                "_id": 0,
                "id_order": 1,
                "city": 1,
                "order_date": 1,
                "status": 1,
                "total_amount": 1,
            },
        )
        .sort("order_date", -1)
        .limit(limit)
    )

    return list(cursor)


def query_city_orders_by_date(
    collection,
    city: str,
    start_date: str,
    end_date: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Return orders for one city inside a date range."""

    cursor = (
        collection.find(
            {
                "city": city,
                "order_date": {
                    "$gte": start_date,
                    "$lt": end_date,
                },
            },
            {
                "_id": 0,
                "id_order": 1,
                "city": 1,
                "order_date": 1,
                "status": 1,
                "total_amount": 1,
            },
        )
        .sort("order_date", -1)
        .limit(limit)
    )

    return list(cursor)


def query_customer_orders(
    collection,
    customer_id: str,
    start_date: str,
    end_date: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Return orders for a customer inside a date range."""

    cursor = (
        collection.find(
            {
                "customer_id": customer_id,
                "order_date": {
                    "$gte": start_date,
                    "$lt": end_date,
                },
            },
            {
                "_id": 0,
                "id_order": 1,
                "customer_id": 1,
                "order_date": 1,
                "status": 1,
                "total_amount": 1,
            },
        )
        .sort("order_date", -1)
        .limit(limit)
    )

    return list(cursor)


def query_orders_by_status(
    collection,
    status: str,
    start_date: str,
    end_date: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Return orders with a specific status inside a date range."""

    cursor = (
        collection.find(
            {
                "status": status,
                "order_date": {
                    "$gte": start_date,
                    "$lt": end_date,
                },
            },
            {
                "_id": 0,
                "id_order": 1,
                "status": 1,
                "city": 1,
                "order_date": 1,
                "total_amount": 1,
            },
        )
        .sort("order_date", -1)
        .limit(limit)
    )

    return list(cursor)


def query_orders_by_status_and_city(
    collection,
    status: str,
    city: str,
    start_date: str,
    end_date: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Return orders matching status, city, and date range."""

    cursor = (
        collection.find(
            {
                "status": status,
                "city": city,
                "order_date": {
                    "$gte": start_date,
                    "$lt": end_date,
                },
            },
            {
                "_id": 0,
                "id_order": 1,
                "status": 1,
                "city": 1,
                "order_date": 1,
                "total_amount": 1,
            },
        )
        .sort("order_date", -1)
        .limit(limit)
    )

    return list(cursor)


def find_dynamic_parameters(collection) -> dict[str, str]:
    """
    Choose real values dynamically from the database.

    This avoids hard-coding training-data values and makes the
    queries usable with different datasets.
    """

    first_doc = collection.find_one(
        {},
        {
            "_id": 0,
            "city": 1,
            "customer_id": 1,
            "status": 1,
            "order_date": 1,
        },
    )

    if not first_doc:
        raise RuntimeError("orders_validated is empty.")

    city = first_doc.get("city")
    customer_id = first_doc.get("customer_id")
    status = first_doc.get("status")
    order_date = first_doc.get("order_date")

    if not all([city, customer_id, status, order_date]):
        raise RuntimeError(
            "The sample document does not contain the required fields."
        )

    # Use a broad dynamic range around the observed date.
    year = str(order_date)[:4]

    start_date = f"{year}-01-01"
    end_date = f"{int(year) + 1}-01-01"

    return {
        "city": str(city),
        "customer_id": str(customer_id),
        "status": str(status),
        "start_date": start_date,
        "end_date": end_date,
    }


def explain_query(
    collection,
    filter_document: dict[str, Any],
    sort_document: dict[str, int] | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    """Return MongoDB executionStats for a query."""

    cursor = collection.find(
        filter_document,
        {
            "_id": 0,
            "id_order": 1,
            "city": 1,
            "customer_id": 1,
            "status": 1,
            "order_date": 1,
            "total_amount": 1,
        },
    )

    if sort_document:
        cursor = cursor.sort(list(sort_document.items()))

    cursor = cursor.limit(limit)

    return cursor.explain("executionStats")


def run_demo():
    """Run all five queries using real values from the database."""

    client, collection = get_collection()

    try:
        params = find_dynamic_parameters(collection)

        print("=" * 80)
        print("QUERY DEMONSTRATION")
        print("=" * 80)

        print("\nDynamic parameters:")
        for key, value in params.items():
            print(f"{key}: {value}")

        print("\n1. Recent orders by city")
        result = query_recent_orders_by_city(
            collection,
            params["city"],
        )
        print(f"Returned: {len(result)}")
        for row in result:
            print(row)

        print("\n2. Orders by city and date range")
        result = query_city_orders_by_date(
            collection,
            params["city"],
            params["start_date"],
            params["end_date"],
        )
        print(f"Returned: {len(result)}")
        for row in result:
            print(row)

        print("\n3. Customer orders by date range")
        result = query_customer_orders(
            collection,
            params["customer_id"],
            params["start_date"],
            params["end_date"],
        )
        print(f"Returned: {len(result)}")
        for row in result:
            print(row)

        print("\n4. Orders by status and date range")
        result = query_orders_by_status(
            collection,
            params["status"],
            params["start_date"],
            params["end_date"],
        )
        print(f"Returned: {len(result)}")
        for row in result:
            print(row)

        print("\n5. Orders by status + city + date range")
        result = query_orders_by_status_and_city(
            collection,
            params["status"],
            params["city"],
            params["start_date"],
            params["end_date"],
        )
        print(f"Returned: {len(result)}")
        for row in result:
            print(row)

        print("\n" + "=" * 80)
        print("QUERY INDEXES TO ADD IN NEXT STEP")
        print("=" * 80)

        for name, keys in QUERY_INDEXES.items():
            print(f"{name}: {keys}")

    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(
        description="Run practical MongoDB queries for the final project."
    )

    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run the five practical query demonstrations.",
    )

    args = parser.parse_args()

    if args.demo:
        run_demo()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()