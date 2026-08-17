import argparse
import sys
import time
import uuid
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    struct,
)
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)


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
    SPARK_MASTER,
    SPARK_APP_NAME,
)

from src.mongo_setup import (
    get_mongo_client,
    get_database,
    get_raw_collection,
)


# =========================================================
# FIXED RAW SCHEMA
# =========================================================

RAW_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_phone", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("delivery_type", StringType(), True),
        StructField("delivery_cost", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_amount", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("items_json", StringType(), True),
    ]
)


# =========================================================
# SPARK SESSION
# =========================================================

def create_spark_session() -> SparkSession:
    """
    Create SparkSession using project settings.
    """

    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(SPARK_APP_NAME)
        .config(
            "spark.mongodb.write.connection.uri",
            MONGO_URI,
        )
        .config(
            "spark.mongodb.read.connection.uri",
            MONGO_URI,
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# =========================================================
# READ RAW CSV
# =========================================================

def read_raw_csv(
    spark: SparkSession,
    input_path: Path,
):
    """
    Read CSV using fixed String schema.

    No cleaning is performed here.
    """

    dataframe = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .option("quote", '"')
        .option("escape", '"')
        .option(
            "ignoreLeadingWhiteSpace",
            "false",
        )
        .option(
            "ignoreTrailingWhiteSpace",
            "false",
        )
        .option(
            "nullValue",
            "__SPARK_NULL_SENTINEL__",
        )
        .schema(RAW_SCHEMA)
        .csv(
            str(input_path.resolve())
        )
    )

    return dataframe


# =========================================================
# BUILD RAW DATAFRAME
# =========================================================

def build_raw_dataframe(
    raw_dataframe,
    input_path: Path,
    run_id: str,
):
    """
    Add Raw metadata without changing source values.
    """

    raw_columns = [
        field.name
        for field in RAW_SCHEMA.fields
    ]

    dataframe = raw_dataframe.select(

        lit(
            run_id
        ).alias(
            "run_id"
        ),

        lit(
            str(input_path.resolve())
        ).alias(
            "source_file"
        ),

        # Spark CSV reader does not expose
        # exact physical CSV row numbers.
        lit(
            None
        ).cast(
            "long"
        ).alias(
            "source_row_number"
        ),

        current_timestamp().alias(
            "ingested_at"
        ),

        lit(
            "pyspark"
        ).alias(
            "engine_used"
        ),

        struct(
            *raw_columns
        ).alias(
            "raw_record"
        ),
    )

    return dataframe


# =========================================================
# WRITE TO MONGODB
# =========================================================

def write_raw_to_mongodb(
    dataframe,
):
    """
    Write Raw DataFrame through MongoDB Spark Connector.
    """

    (
        dataframe.write
        .format("mongodb")
        .mode("append")
        .option(
            "database",
            MONGO_DB_NAME,
        )
        .option(
            "collection",
            RAW_COLLECTION,
        )
        .save()
    )


# =========================================================
# VERIFY MONGODB COUNT
# =========================================================

def count_loaded_records(
    run_id: str,
) -> int:
    """
    Count documents written by this Spark run.
    """

    client = None

    try:
        client = get_mongo_client()

        database = get_database(
            client
        )

        collection = get_raw_collection(
            database
        )

        return collection.count_documents(
            {
                "run_id": run_id,
                "engine_used": "pyspark",
            }
        )

    finally:
        if client is not None:
            client.close()


# =========================================================
# MAIN SPARK LOADER
# =========================================================

def load_csv_to_raw_with_spark(
    input_path: Path,
    run_id: str,
) -> dict:
    """
    Load CSV into orders_raw using PySpark.
    """

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    if not input_path.is_file():
        raise ValueError(
            f"Input path is not a file: {input_path}"
        )

    spark = None

    total_start = time.perf_counter()

    try:

        spark = create_spark_session()

        print(
            f"Spark version   : "
            f"{spark.version}"
        )

        print(
            f"Spark master    : "
            f"{spark.sparkContext.master}"
        )

        print(
            f"Spark UI        : "
            f"{spark.sparkContext.uiWebUrl}"
        )

        raw_dataframe = read_raw_csv(
            spark=spark,
            input_path=input_path,
        )

        input_partitions = (
            raw_dataframe.rdd.getNumPartitions()
        )

        print(
            f"Input partitions: "
            f"{input_partitions}"
        )

        print()
        print(
            "===== RAW SCHEMA ====="
        )

        raw_dataframe.printSchema()

        # Spark Action
        rows_read = (
            raw_dataframe.count()
        )

        print(
            f"Rows read       : "
            f"{rows_read}"
        )

        mongo_dataframe = (
            build_raw_dataframe(
                raw_dataframe=raw_dataframe,
                input_path=input_path,
                run_id=run_id,
            )
        )

        print()
        print(
            "===== SPARK PLAN ====="
        )

        mongo_dataframe.explain(
            mode="formatted"
        )

        write_start = (
            time.perf_counter()
        )

        write_raw_to_mongodb(
            mongo_dataframe
        )

        write_elapsed = (
            time.perf_counter()
            - write_start
        )

        rows_loaded = (
            count_loaded_records(
                run_id
            )
        )

        total_elapsed = (
            time.perf_counter()
            - total_start
        )

        throughput = (
            rows_loaded / total_elapsed
            if total_elapsed > 0
            else 0.0
        )

        return {
            "run_id": run_id,
            "engine_used": "pyspark",
            "rows_read": rows_read,
            "rows_loaded": rows_loaded,
            "input_partitions": input_partitions,
            "write_seconds": write_elapsed,
            "elapsed_seconds": total_elapsed,
            "throughput": throughput,
            "spark_version": spark.version,
            "spark_master": spark.sparkContext.master,
        }

    finally:

        if spark is not None:
            spark.stop()


# =========================================================
# COMMAND LINE ARGUMENTS
# =========================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "Load a CSV file into MongoDB orders_raw "
            "using PySpark DataFrame API."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to input CSV file.",
    )

    return parser.parse_args()


# =========================================================
# MAIN
# =========================================================

def main():

    args = parse_arguments()

    run_id = str(
        uuid.uuid4()
    )

    file_size_mb = (
        args.input.stat().st_size
        / (1024 * 1024)
        if args.input.exists()
        else 0.0
    )

    print(
        "===== PYSPARK RAW LOADER ====="
    )

    print(
        f"Run ID          : "
        f"{run_id}"
    )

    print(
        f"Input file      : "
        f"{args.input}"
    )

    print(
        f"File size MB    : "
        f"{file_size_mb:.2f}"
    )

    print(
        "Engine          : pyspark"
    )

    print()

    try:

        result = (
            load_csv_to_raw_with_spark(
                input_path=args.input,
                run_id=run_id,
            )
        )

        print()
        print(
            "===== FINAL RESULT ====="
        )

        print(
            f"Run ID           : "
            f"{result['run_id']}"
        )

        print(
            f"Rows read        : "
            f"{result['rows_read']}"
        )

        print(
            f"Rows loaded raw  : "
            f"{result['rows_loaded']}"
        )

        print(
            f"Input partitions : "
            f"{result['input_partitions']}"
        )

        print(
            f"Write seconds    : "
            f"{result['write_seconds']:.2f}"
        )

        print(
            f"Elapsed seconds  : "
            f"{result['elapsed_seconds']:.2f}"
        )

        print(
            f"Throughput       : "
            f"{result['throughput']:.2f} rows/sec"
        )

        if (
            result["rows_read"]
            == result["rows_loaded"]
        ):
            print(
                "RAW LOAD CONSISTENCY: PASS"
            )
        else:
            print(
                "RAW LOAD CONSISTENCY: FAIL"
            )

        print(
            "PYSPARK RAW LOAD: PASS"
        )

    except Exception as error:

        print()

        print(
            "PYSPARK RAW LOAD: FAIL"
        )

        print(
            f"ERROR TYPE: "
            f"{type(error).__name__}"
        )

        print(
            f"ERROR: {error}"
        )

        raise


if __name__ == "__main__":
    main()