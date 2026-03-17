import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType,
    MapType,
    _parse_datatype_string,
)



# -- MapType schema -------------------------------------------------------
schema_with_map = StructType([
    StructField("id",         LongType(),                           nullable=False),
    StructField("attributes", MapType(StringType(), StringType()),  nullable=True),
])

ORDERS  = [(1, "Alice", 99.99), (2, "Bob", 149.00)]
MAP_DATA = [
    (1, {"color": "red",   "size": "L"}),
    (2, {"color": "blue",  "size": "M"}),
    (3, {"color": "green", "size": "S"}),
]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("schema-definition-type2")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")

    # -- DDL string → StructType via _parse_datatype_string -------------------
    DDL = "struct<order_id:bigint not null,customer:string,amount:double>"
    schema_from_ddl = _parse_datatype_string(DDL)

    # -- fromDDL ----------------------------------------------------------
    print("=== fromDDL ===")
    print("simpleString :", schema_from_ddl.simpleString())
    df_ddl = spark.createDataFrame(ORDERS, schema=schema_from_ddl)
    df_ddl.show()
    df_ddl.printSchema()

    # -- MapType ----------------------------------------------------------
    print("=== MapType ===")
    df_map = spark.createDataFrame(MAP_DATA, schema=schema_with_map)
    df_map.show(truncate=False)
    df_map.printSchema()
    print("simpleString :", schema_with_map.simpleString())
    print("jsonValue    :", json.dumps(schema_with_map.jsonValue(), indent=2))

    # -- JSON round-trip --------------------------------------------------
    print("=== JSON round-trip ===")
    schema_back = StructType.fromJson(json.loads(schema_with_map.json()))
    print("round-trip ok:", schema_with_map == schema_back)

    spark.stop()

