import sys
from pathlib import Path
from typing import Any, Dict, List

from pymongo import (
    MongoClient,
    ASCENDING,
    UpdateOne,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    VALIDATED_COLLECTION,
)

IGNORED_COMPARISON_FIELDS = {
    "_id",
    "id_run",
    "file_source",
    "number_row_source",
    "at_ingested",
    "engine_used",
}


def get_mongo_client():

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=10000,
        socketTimeoutMS=120000,
        retryWrites=True
    )

    client.admin.command("ping")

    return client


def ensure_unique_index(collection):

    collection.create_index(
        [("id_order", ASCENDING)],
        unique=True,
        name="unique_id_order"
    )


def get_order_id(
    document: Dict[str, Any]
):

    value = document.get("id_order")

    if value not in (None, ""):
        return str(value).strip()

    raw = document.get(
        "record_raw",
        {}
    )

    for field in (
        "id_order",
        "order_id",
        "orderid"
    ):

        value = raw.get(field)

        if value not in (None, ""):
            return str(value).strip()

    return None


def prepare_document(
    document: Dict[str, Any]
):

    prepared = dict(document)

    prepared.pop(
        "_id",
        None
    )

    return prepared


def normalize_for_comparison(
    document: Dict[str, Any]
):

    normalized = dict(
        document
    )

    for field in IGNORED_COMPARISON_FIELDS:

        normalized.pop(
            field,
            None
        )

    return normalized


def business_documents_equal(
    old_document: Dict[str, Any],
    new_document: Dict[str, Any]
):

    old_normalized = normalize_for_comparison(
        old_document
    )

    new_normalized = normalize_for_comparison(
        new_document
    )

    return old_normalized == new_normalized


def upsert_one(
    collection,
    document: Dict[str, Any]
):

    order_id = get_order_id(
        document
    )

    if order_id is None:

        raise ValueError(
            "Cannot upsert document: "
            "id_order is missing."
        )

    new_document = prepare_document(
        document
    )

    new_document["id_order"] = order_id

    existing = collection.find_one(
        {
            "id_order": order_id
        }
    )

    if existing is None:

        collection.update_one(
            {
                "id_order": order_id
            },
            {
                "$set": new_document
            },
            upsert=True
        )

        return "inserted"

    if business_documents_equal(
        existing,
        new_document
    ):

        return "unchanged"

    collection.update_one(
        {
            "id_order": order_id
        },
        {
            "$set": new_document
        },
        upsert=True
    )

    return "updated"


def upsert_documents(
    documents: List[Dict[str, Any]]
):

    if not documents:

        return {
            "count_inserted": 0,
            "count_updated": 0,
            "count_unchanged": 0,
            "count_processed": 0,
        }

    client = get_mongo_client()

    try:

        db = client[
            MONGO_DATABASE
        ]

        collection = db[
            VALIDATED_COLLECTION
        ]

        ensure_unique_index(
            collection
        )

        prepared_documents = []
        order_ids = []

        for document in documents:

            order_id = get_order_id(
                document
            )

            if order_id is None:
                continue

            new_document = prepare_document(
                document
            )

            new_document["id_order"] = order_id

            prepared_documents.append(
                new_document
            )

            order_ids.append(
                order_id
            )

        if not prepared_documents:

            return {
                "count_inserted": 0,
                "count_updated": 0,
                "count_unchanged": 0,
                "count_processed": 0,
            }

        existing_documents = {}

        cursor = collection.find(
            {
                "id_order": {
                    "$in": order_ids
                }
            }
        )

        for existing in cursor:

            existing_id = existing.get(
                "id_order"
            )

            if existing_id is not None:

                existing_documents[
                    str(existing_id)
                ] = existing

        operations = []

        count_inserted = 0
        count_updated = 0
        count_unchanged = 0

        for document in prepared_documents:

            order_id = document[
                "id_order"
            ]

            existing = existing_documents.get(
                order_id
            )

            if existing is None:

                operations.append(
                    UpdateOne(
                        {
                            "id_order": order_id
                        },
                        {
                            "$set": document
                        },
                        upsert=True
                    )
                )

                count_inserted += 1

                continue

            if business_documents_equal(
                existing,
                document
            ):

                count_unchanged += 1

                continue

            operations.append(
                UpdateOne(
                    {
                        "id_order": order_id
                    },
                    {
                        "$set": document
                    },
                    upsert=True
                )
            )

            count_updated += 1

        if operations:

            collection.bulk_write(
                operations,
                ordered=False
            )

        return {
            "count_inserted": count_inserted,
            "count_updated": count_updated,
            "count_unchanged": count_unchanged,
            "count_processed": len(
                prepared_documents
            ),
        }

    finally:

        client.close()