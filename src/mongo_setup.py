from pathlib import Path
import sys

from pymongo import ASCENDING, MongoClient
from pymongo.database import Database
from pymongo.collection import Collection


# =========================================================
# PROJECT IMPORTS
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import (
    MONGO_URI,
    MONGO_DB_NAME,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)


# =========================================================
# VALIDATED COLLECTION SCHEMA
# =========================================================

VALIDATED_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",

        "required": [
            "order_id",
            "customer_id",
            "quality_status",
            "run_id",
            "validated_at",
        ],

        "properties": {

            "order_id": {
                "bsonType": "string",
                "minLength": 1,
                "description": (
                    "Business key for the order."
                ),
            },

            "customer_id": {
                "bsonType": "string",
                "minLength": 1,
            },

            "quality_status": {
                "enum": [
                    "Valid",
                    "Corrected",
                ],
            },

            "run_id": {
                "bsonType": "string",
            },

            "validated_at": {
                "bsonType": "date",
            },

            "corrections": {
                "bsonType": "array",
            },

            "source_file": {
                "bsonType": [
                    "string",
                    "null",
                ],
            },

            "source_row_number": {
                "bsonType": [
                    "int",
                    "long",
                    "null",
                ],
            },

            "engine_used": {
                "bsonType": [
                    "string",
                    "null",
                ],
            },
        },
    }
}


# =========================================================
# CONNECTION
# =========================================================

def get_mongo_client() -> MongoClient:
    """
    Create MongoDB client and verify server connection.
    """

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )

    client.admin.command(
        "ping"
    )

    return client


# =========================================================
# DATABASE
# =========================================================

def get_database(
    client: MongoClient,
) -> Database:
    """
    Return project database.
    """

    return client[
        MONGO_DB_NAME
    ]


# =========================================================
# COLLECTION GETTERS
# =========================================================

def get_raw_collection(
    database: Database,
) -> Collection:
    """
    Return orders_raw.
    """

    return database[
        RAW_COLLECTION
    ]


def get_validated_collection(
    database: Database,
) -> Collection:
    """
    Return orders_validated.
    """

    return database[
        VALIDATED_COLLECTION
    ]


def get_quarantine_collection(
    database: Database,
) -> Collection:
    """
    Return orders_quarantine.
    """

    return database[
        QUARANTINE_COLLECTION
    ]


# =========================================================
# CREATE / UPDATE VALIDATED COLLECTION
# =========================================================

def setup_validated_collection(
    database: Database,
):
    """
    Create orders_validated with schema validation.

    If it already exists, update its validator.
    """

    collection_names = (
        database.list_collection_names()
    )

    # -----------------------------------------------------
    # CREATE COLLECTION
    # -----------------------------------------------------

    if (
        VALIDATED_COLLECTION
        not in collection_names
    ):

        database.create_collection(
            VALIDATED_COLLECTION,
            validator=VALIDATED_VALIDATOR,
            validationLevel="strict",
            validationAction="error",
        )

        print(
            "Validated collection created."
        )

    # -----------------------------------------------------
    # UPDATE EXISTING VALIDATOR
    # -----------------------------------------------------

    else:

        database.command(
            {
                "collMod": VALIDATED_COLLECTION,

                "validator": (
                    VALIDATED_VALIDATOR
                ),

                "validationLevel": "strict",

                "validationAction": "error",
            }
        )

        print(
            "Validated collection validator updated."
        )

    collection = database[
        VALIDATED_COLLECTION
    ]

    # -----------------------------------------------------
    # UNIQUE BUSINESS KEY
    # -----------------------------------------------------

    index_name = collection.create_index(
        [
            (
                "order_id",
                ASCENDING,
            )
        ],
        unique=True,
        name="ux_order_id",
    )

    print(
        f"Unique index: {index_name}"
    )


# =========================================================
# QUARANTINE COLLECTION
# =========================================================

def setup_quarantine_collection(
    database: Database,
):
    """
    Ensure orders_quarantine exists.

    Quarantine remains flexible because different
    errors may require different diagnostic fields.
    """

    collection_names = (
        database.list_collection_names()
    )

    if (
        QUARANTINE_COLLECTION
        not in collection_names
    ):

        database.create_collection(
            QUARANTINE_COLLECTION
        )

        print(
            "Quarantine collection created."
        )

    else:

        print(
            "Quarantine collection already exists."
        )


# =========================================================
# RAW COLLECTION
# =========================================================

def check_raw_collection(
    database: Database,
):
    """
    Verify that orders_raw exists.
    """

    collection_names = (
        database.list_collection_names()
    )

    if (
        RAW_COLLECTION
        not in collection_names
    ):

        raise RuntimeError(
            "orders_raw does not exist."
        )

    print(
        "Raw collection exists."
    )


# =========================================================
# FULL DATABASE SETUP
# =========================================================

def setup_project_database():
    """
    Prepare all required MongoDB collections.
    """

    client = None

    try:

        client = get_mongo_client()

        database = get_database(
            client
        )

        print(
            "===== MONGODB PROJECT SETUP ====="
        )

        print(
            f"Database: {database.name}"
        )

        print()

        # Raw must already exist
        check_raw_collection(
            database
        )

        # Validated
        setup_validated_collection(
            database
        )

        # Quarantine
        setup_quarantine_collection(
            database
        )

        print()
        print(
            "MONGODB PROJECT SETUP: PASS"
        )

    finally:

        if client is not None:
            client.close()


# =========================================================
# VERIFY SETUP
# =========================================================

def verify_project_database():
    """
    Verify collections and unique order_id index.
    """

    client = None

    try:

        client = get_mongo_client()

        database = get_database(
            client
        )

        collection_names = (
            database.list_collection_names()
        )

        raw_exists = (
            RAW_COLLECTION
            in collection_names
        )

        validated_exists = (
            VALIDATED_COLLECTION
            in collection_names
        )

        quarantine_exists = (
            QUARANTINE_COLLECTION
            in collection_names
        )

        validated_collection = (
            get_validated_collection(
                database
            )
        )

        indexes = list(
            validated_collection.list_indexes()
        )

        unique_order_index = any(

            index.get("name")
            == "ux_order_id"

            and index.get(
                "unique",
                False,
            )

            for index in indexes
        )

        print()
        print(
            "===== MONGODB VERIFICATION ====="
        )

        print(
            f"Raw exists        : {raw_exists}"
        )

        print(
            f"Validated exists  : {validated_exists}"
        )

        print(
            f"Quarantine exists : {quarantine_exists}"
        )

        print(
            f"Unique order_id   : {unique_order_index}"
        )

        passed = all(
            [
                raw_exists,
                validated_exists,
                quarantine_exists,
                unique_order_index,
            ]
        )

        print()

        print(
            "DATABASE VERIFICATION:",
            "PASS" if passed else "FAIL",
        )

        return passed

    finally:

        if client is not None:
            client.close()


# =========================================================
# MAIN
# =========================================================

def main():

    setup_project_database()

    verify_project_database()


if __name__ == "__main__":
    main()