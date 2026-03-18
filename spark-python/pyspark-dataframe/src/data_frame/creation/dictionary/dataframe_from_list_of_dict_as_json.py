"""
Create a DataFrame by reading a list of JSON-serialisable dicts via sparkContext.parallelize.
This mirrors how spark.read.json() handles streaming JSON strings.
"""

import json

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    records = [
        {"type_activity_id": 1, "type_activity_name": "Cycling"},
        {"type_activity_id": 2, "type_activity_name": "Running"},
        {"type_activity_id": 3, "type_activity_name": "Swimming"},
    ]

    # Parallelise as JSON strings then read back with inferred schema
    json_rdd = spark.sparkContext.parallelize([json.dumps(r) for r in records])
    df = spark.read.json(json_rdd)
    df.printSchema()
    df.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("creation-from-json")
    main(spark)
    spark.stop()
