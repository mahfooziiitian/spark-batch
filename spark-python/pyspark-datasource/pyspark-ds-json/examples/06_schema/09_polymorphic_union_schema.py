"""Polymorphic JSON — union schemas for mixed-type records.

Demonstrates handling JSON files where records share common fields but have
different data shapes depending on a type discriminator. The schema is defined
as a superset (union) of all possible fields — irrelevant fields will be null.

Key concepts:
    - Union/superset schema: define ALL possible fields across types
    - Nullable fields for type-specific attributes
    - Common fields (e.g., "type", "date") are non-nullable
    - Nested structs and arrays within polymorphic data

Pattern:
    [
      {"common": {"type": "A", "date": "..."}, "data": {"name": "...", "pets": [...]}},
      {"common": {"type": "B", "date": "..."}, "data": {"whatever": {...}, "favoriteInts": [...]}}
    ]

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pys_json import DATA_HOME, get_spark, set_log_level
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.polymorphic")


if __name__ == "__main__":
    spark = get_spark("polymorphic-json")

    # Data schema: superset of all type-specific fields
    data_schema = StructType(
        [
            # Type A fields
            StructField("name", StringType(), nullable=True),
            StructField("pets", ArrayType(StringType()), nullable=True),
            # Type B fields
            StructField(
                "whatever",
                StructType(
                    [
                        StructField(
                            "X",
                            StructType(
                                [
                                    StructField("foo", IntegerType(), nullable=True),
                                ]
                            ),
                            nullable=True,
                        ),
                        StructField("Y", StringType(), nullable=True),
                    ]
                ),
                nullable=True,
            ),
            StructField("favoriteInts", ArrayType(IntegerType()), nullable=True),
        ]
    )

    # Common fields shared by all record types
    common_schema = StructType(
        [
            StructField("type", StringType(), nullable=False),
            StructField("date", TimestampType(), nullable=False),
        ]
    )

    # Top-level schema combining common + data
    schema = StructType(
        [
            StructField("common", common_schema, nullable=False),
            StructField("data", data_schema, nullable=False),
        ]
    )

    logger.info("Polymorphic schema: %s", schema.simpleString())
    logger.debug("Schema JSON:\n%s", schema.json())

    data_file = DATA_HOME + "/file_data/json/dynamic_keys/polymorphic.json"
    logger.info("Reading: %s", data_file)

    df = spark.read.format("json").option("multiLine", True).schema(schema).load(data_file)

    df.show(truncate=False)
    df.printSchema()

    # Filter by type to show type-specific data
    logger.info("Type A records:")
    df.filter(df.common.type == "A").select("common.type", "data.name", "data.pets").show()

    logger.info("Type B records:")
    df.filter(df.common.type == "B").select("common.type", "data.whatever", "data.favoriteInts").show()

    spark.stop()
