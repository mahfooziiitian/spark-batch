"""
Parse a DataFrame schema from a JSON file using StructType.fromJson().
"""

import json
from pathlib import Path

from pyspark.sql.types import StructType

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    data = [
        (("James", "", "Smith"), "36636", "M", 3100),
        (("Michael", "Rose", ""), "40288", "M", 4300),
        (("Robert", "", "Williams"), "42114", "M", 1400),
        (("Maria", "Anne", "Jones"), "39192", "F", 5500),
        (("Jen", "Mary", "Brown"), "", "F", -1),
    ]

    schema_file = Path(__file__).parent / "schema.json"
    with open(schema_file) as fh:
        schema = StructType.fromJson(json.load(fh))

    df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.printSchema()
    df.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("parse-json-schema")
    main(spark)
    spark.stop()
