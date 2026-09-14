import sys
from pathlib import Path


# ============================================================
# Project Root
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================
# MongoDB
# ============================================================

from pymongo import MongoClient, ASCENDING
from pymongo.errors import OperationFailure


# ============================================================
# Configuration
# ============================================================

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)


# ============================================================
# MongoDB Connection
# ============================================================

def get_client():

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000
    )

    client.admin.command("ping")

    return client


# ============================================================
# Raw Schema
# ============================================================

RAW_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",

        "required": [
            "id_run",
            "record_raw"
        ],

        "properties": {

            "id_run": {
                "bsonType": "string"
            },

            "record_raw": {
                "bsonType": "object"
            },

            "file_source": {
                "bsonType": "string"
            },

            "number_row_source": {
                "bsonType": [
                    "int",
                    "long"
                ]
            },

            "at_ingested": {
                "bsonType": "date"
            },

            "engine_used": {
                "bsonType": "string"
            }
        }
    }
}


# ============================================================
# Validated Schema
# ============================================================

VALIDATED_VALIDATOR = {
    "$jsonSchema": {

        "bsonType": "object",

        "required": [
            "id_order",
            "quality_status",
            "record_raw"
        ],

        "properties": {

            "id_order": {
                "bsonType": "string",
                "description": (
                    "Stable business key"
                )
            },

            "quality_status": {
                "enum": [
                    "valid",
                    "corrected"
                ]
            },

            "record_raw": {
                "bsonType": "object"
            },

            "codes_error": {
                "bsonType": "array",
                "items": {
                    "bsonType": "string"
                }
            },

            "details_error": {
                "bsonType": "array",
                "items": {
                    "bsonType": "string"
                }
            },

            "corrections": {
                "bsonType": "array"
            }
        }
    }
}


# ============================================================
# Quarantine Schema
# ============================================================

QUARANTINE_VALIDATOR = {
    "$jsonSchema": {

        "bsonType": "object",

        "required": [
            "quality_status",
            "record_raw",
            "codes_error",
            "details_error"
        ],

        "properties": {

            "quality_status": {
                "enum": [
                    "quarantined"
                ]
            },

            "record_raw": {
                "bsonType": "object"
            },

            "codes_error": {
                "bsonType": "array",

                "minItems": 1,

                "items": {
                    "bsonType": "string"
                }
            },

            "details_error": {
                "bsonType": "array",

                "minItems": 1,

                "items": {
                    "bsonType": "string"
                }
            }
        }
    }
}


# ============================================================
# Create / Update Collection
# ============================================================

def create_or_update_collection(
    db,
    collection_name,
    validator
):

    existing_collections = db.list_collection_names()

    if collection_name not in existing_collections:

        db.create_collection(
            collection_name,
            validator=validator,
            validationLevel="strict",
            validationAction="error"
        )

        print(
            f"[CREATED] {collection_name}"
        )

    else:

        db.command(
            "collMod",
            collection_name,

            validator=validator,

            validationLevel="strict",

            validationAction="error"
        )

        print(
            f"[UPDATED] {collection_name}"
        )


# ============================================================
# Create Indexes
# ============================================================

def create_indexes(db):

    raw = db[
        RAW_COLLECTION
    ]

    validated = db[
        VALIDATED_COLLECTION
    ]

    quarantine = db[
        QUARANTINE_COLLECTION
    ]

    # --------------------------------------------------------
    # Raw
    # --------------------------------------------------------

    raw.create_index(
        [
            ("id_run", ASCENDING)
        ],
        name="idx_id_run"
    )

    raw.create_index(
        [
            ("id_run", ASCENDING),
            ("number_row_source", ASCENDING)
        ],
        name="idx_run_row"
    )

    # --------------------------------------------------------
    # Validated
    # --------------------------------------------------------

    validated.create_index(
        [
            ("id_order", ASCENDING)
        ],
        unique=True,
        name="unique_id_order"
    )

    validated.create_index(
        [
            ("quality_status", ASCENDING)
        ],
        name="idx_quality_status"
    )

    # --------------------------------------------------------
    # Quarantine
    # --------------------------------------------------------

    quarantine.create_index(
        [
            ("codes_error", ASCENDING)
        ],
        name="idx_codes_error"
    )

    quarantine.create_index(
        [
            ("id_run", ASCENDING)
        ],
        name="idx_quarantine_id_run"
    )

    print()
    print("[INDEXES] Created successfully")


# ============================================================
# Display Indexes
# ============================================================

def show_indexes(db):

    print()
    print("=" * 75)
    print("MONGODB INDEXES")
    print("=" * 75)

    for collection_name in [
        RAW_COLLECTION,
        VALIDATED_COLLECTION,
        QUARANTINE_COLLECTION
    ]:

        collection = db[
            collection_name
        ]

        print()
        print(
            f"Collection: "
            f"{collection_name}"
        )

        for index in collection.list_indexes():

            print(
                f"  - {index['name']}"
            )

            print(
                f"    key={index['key']}"
            )

            if index.get("unique"):

                print(
                    "    unique=True"
                )

    print("=" * 75)


# ============================================================
# Display Collections
# ============================================================

def show_collections(db):

    print()
    print("=" * 75)
    print("MONGODB COLLECTIONS")
    print("=" * 75)

    for name in db.list_collection_names():

        count = db[
            name
        ].count_documents({})

        print(
            f"{name:<25} "
            f"{count:,} documents"
        )

    print("=" * 75)


# ============================================================
# Test Unique Index
# ============================================================

def test_unique_index(db):

    collection = db[
        VALIDATED_COLLECTION
    ]

    print()
    print(
        "Testing Unique Index..."
    )

    test_id = (
        "__UNIQUE_INDEX_TEST__"
    )

    collection.delete_many(
        {
            "id_order": test_id
        }
    )

    document = {
        "id_order": test_id,

        "quality_status": "valid",

        "record_raw": {
            "test": True
        },

        "codes_error": [],

        "details_error": [],

        "corrections": []
    }

    collection.insert_one(
        document
    )

    try:

        collection.insert_one(
            document
        )

        print(
            "[FAIL] Duplicate was accepted."
        )

    except Exception:

        print(
            "[PASS] Duplicate was rejected."
        )

    finally:

        collection.delete_many(
            {
                "id_order": test_id
            }
        )


# ============================================================
# Main Setup
# ============================================================

def main():

    client = get_client()

    try:

        db = client[
            MONGO_DATABASE
        ]

        print()
        print("=" * 75)
        print("MONGODB DATABASE SETUP")
        print("=" * 75)

        print(
            f"Database: "
            f"{MONGO_DATABASE}"
        )

        # ----------------------------------------------------
        # Collections
        # ----------------------------------------------------

        create_or_update_collection(
            db,
            RAW_COLLECTION,
            RAW_VALIDATOR
        )

        create_or_update_collection(
            db,
            VALIDATED_COLLECTION,
            VALIDATED_VALIDATOR
        )

        create_or_update_collection(
            db,
            QUARANTINE_COLLECTION,
            QUARANTINE_VALIDATOR
        )

        # ----------------------------------------------------
        # Indexes
        # ----------------------------------------------------

        create_indexes(
            db
        )

        # ----------------------------------------------------
        # Verification
        # ----------------------------------------------------

        show_indexes(
            db
        )

        show_collections(
            db
        )

        test_unique_index(
            db
        )

        print()
        print(
            "[SUCCESS] MongoDB setup completed."
        )

    except OperationFailure as error:

        print()
        print(
            "[MONGODB ERROR]"
        )

        print(error)

        sys.exit(1)

    except Exception as error:

        print()
        print(
            "[ERROR]"
        )

        print(error)

        sys.exit(1)

    finally:

        client.close()


if __name__ == "__main__":
    main()