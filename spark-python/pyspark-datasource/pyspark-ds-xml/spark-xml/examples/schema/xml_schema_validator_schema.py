import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_xml.util.sample_data import ensure_orders_xml, ensure_orders_xsd

os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("spark-xml-validator").getOrCreate()

    # data path
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xmlXsd = str(ensure_orders_xsd(data_home / "file_data" / "xml" / "orders.xsd"))
    xmlFile = str(ensure_orders_xml(data_home / "file_data" / "xml" / "orders.xml"))

    # adding spark context
    spark.sparkContext.addFile(xmlXsd)
    schema = StructType(
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
                                        StructField("Fax", StringType(), True),
                                        StructField(
                                            "FullAddress",
                                            StructType(
                                                [
                                                    StructField("Address", StringType(), True),
                                                    StructField("City", StringType(), True),
                                                    StructField("Country", StringType(), True),
                                                    StructField("PostalCode", LongType(), True),
                                                    StructField("Region", StringType(), True),
                                                ]
                                            ),
                                            True,
                                        ),
                                        StructField("Phone", StringType(), True),
                                        StructField("_CustomerID", StringType(), True),
                                    ]
                                ),
                                True,
                            ),
                            True,
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
                                        StructField("RequiredDate", TimestampType(), True),
                                        StructField(
                                            "ShipInfo",
                                            StructType(
                                                [
                                                    StructField("Freight", DoubleType(), True),
                                                    StructField(
                                                        "ShipAddress",
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField("ShipCity", StringType(), True),
                                                    StructField(
                                                        "ShipCountry",
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField("ShipName", StringType(), True),
                                                    StructField(
                                                        "ShipPostalCode",
                                                        LongType(),
                                                        True,
                                                    ),
                                                    StructField("ShipRegion", StringType(), True),
                                                    StructField("ShipVia", LongType(), True),
                                                    StructField(
                                                        "_ShippedDate",
                                                        TimestampType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        )
                    ]
                ),
                True,
            ),
        ]
    )
    #
    orders = (
        spark.read.format("xml")
        .option("rowTag", "Root")
        .schema(schema)  # .option("rowValidationXSDPath", "orders.xsd").
        .load(xmlFile)
    )

    orders.printSchema()

    print(orders.schema)
