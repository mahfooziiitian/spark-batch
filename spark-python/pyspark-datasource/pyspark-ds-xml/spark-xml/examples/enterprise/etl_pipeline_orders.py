"""Enterprise-style XML ingestion pipeline.

Demonstrates patterns expected in a production Spark batch job:

* a shared, configurable :class:`SparkSession` factory (``get_spark_session``)
* structured logging instead of bare ``print`` statements
* an explicit, enforced read schema (no ``inferSchema`` surprises)
* corrupt-record quarantine: malformed rows are captured in a
  ``_corrupt_record`` column and written to a separate "rejects" location
  instead of silently dropped or failing the whole job
* an idempotent, overwritable Parquet sink partitioned by ingestion date

Run:
    uv run --package spark-xml python examples/enterprise/etl_pipeline_orders.py
"""

import logging
import os
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_xml.util.sample_data import ensure_orders_xml
from spark_xml.util.session.spark_session_util import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger("etl_pipeline_orders")

# Explicit schema plus a corrupt-record column: any row that fails to parse
# against this schema is captured here instead of failing the whole batch.
ORDERS_SCHEMA = StructType(
    [
        StructField(
            "Customers",
            StructType(
                [
                    StructField(
                        "Customer",
                        ArrayType(
                            StructType(
                                [
                                    StructField("CompanyName", StringType(), True),
                                    StructField("ContactName", StringType(), True),
                                    StructField("ContactTitle", StringType(), True),
                                    StructField("Phone", StringType(), True),
                                    StructField("_CustomerID", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                    )
                ]
            ),
            True,
        ),
        StructField(
            "Orders",
            StructType(
                [
                    StructField(
                        "Order",
                        ArrayType(
                            StructType(
                                [
                                    StructField("CustomerID", StringType(), True),
                                    StructField("EmployeeID", LongType(), True),
                                    StructField("OrderDate", TimestampType(), True),
                                    StructField(
                                        "ShipInfo",
                                        StructType([StructField("Freight", DoubleType(), True)]),
                                        True,
                                    ),
                                ]
                            ),
                            True,
                        ),
                    )
                ]
            ),
            True,
        ),
        StructField("_corrupt_record", StringType(), True),
    ]
)


def ingest_orders(data_home: Path, output_dir: Path) -> None:
    spark = get_spark_session(app_name="etl-pipeline-orders")

    xml_file = str(ensure_orders_xml(data_home / "file_data" / "xml" / "orders.xml"))
    logger.info("Reading orders XML from %s", xml_file)

    raw = (
        spark.read.format("xml")
        .option("rowTag", "Root")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(ORDERS_SCHEMA)
        .load(xml_file)
        .cache()  # required: Spark disallows querying only the corrupt-record column lazily
    )

    rejects = raw.filter(F.col("_corrupt_record").isNotNull())
    reject_count = rejects.count()
    if reject_count:
        rejects_path = output_dir / "rejects" / f"ingestion_date={date.today().isoformat()}"
        rejects_path.mkdir(parents=True, exist_ok=True)
        logger.warning("Quarantining %d corrupt record(s) to %s", reject_count, rejects_path)
        rejects.select("_corrupt_record").write.mode("overwrite").json(str(rejects_path))
    else:
        logger.info("No corrupt records found.")

    good = raw.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
    orders = good.select(F.explode("Orders.Order").alias("order")).select(
        "order.CustomerID",
        "order.EmployeeID",
        "order.OrderDate",
        F.col("order.ShipInfo.Freight").alias("Freight"),
    )

    curated_path = output_dir / "curated" / "orders" / f"ingestion_date={date.today().isoformat()}"
    curated_path.mkdir(parents=True, exist_ok=True)
    row_count = orders.count()
    logger.info("Writing %d curated order row(s) to %s", row_count, curated_path)
    orders.write.mode("overwrite").parquet(str(curated_path))

    orders.show(truncate=False)
    spark.stop()
    logger.info("Pipeline complete.")


if __name__ == "__main__":
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    output_dir = Path(os.environ.get("OUTPUT_HOME", "/tmp/spark-xml-output")) / "etl_pipeline_orders"
    ingest_orders(data_home, output_dir)
