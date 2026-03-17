import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# Simulated schema stored in an external registry or config file.
# The dict follows the same format produced by schema.jsonValue().
SCHEMA_REGISTRY: dict = {
    "type": "struct",
    "fields": [
        {"name": "order_id",  "type": "long",   "nullable": False, "metadata": {}},
        {"name": "customer",  "type": "string", "nullable": True,  "metadata": {}},
        {"name": "amount",    "type": "double", "nullable": True,  "metadata": {}},
        {"name": "region",    "type": "string", "nullable": True,  "metadata": {}},
    ],
}

SAMPLE_DATA = [
    (1, "Alice", 99.99,  "North"),
    (2, "Bob",   149.00, "South"),
    (3, "Carol",  75.25, "East"),
]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("schema-from-json")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # -- Load schema from dict (registry lookup) --------------------------
    schema = StructType.fromJson(SCHEMA_REGISTRY)
    print("=== schema from registry dict ===")
    schema.printTreeString()

    df = spark.createDataFrame(SAMPLE_DATA, schema=schema)
    df.show()
    df.printSchema()

    # -- Serialise to JSON string (for storage or transport) --------------
    schema_json_str = schema.json()
    print("=== serialised JSON ===")
    print(json.dumps(json.loads(schema_json_str), indent=2))

    # -- Roundtrip: JSON string → StructType ------------------------------
    schema_restored = StructType.fromJson(json.loads(schema_json_str))
    print("=== roundtrip equal ===", schema == schema_restored)

    # -- Evolve the registry schema (add a column) ------------------------
    evolved = dict(SCHEMA_REGISTRY)
    evolved["fields"] = SCHEMA_REGISTRY["fields"] + [
        {"name": "discount", "type": "double", "nullable": True, "metadata": {}}
    ]
    schema_v2 = StructType.fromJson(evolved)
    print("=== evolved schema (added 'discount') ===")
    schema_v2.printTreeString()

    spark.stop()
