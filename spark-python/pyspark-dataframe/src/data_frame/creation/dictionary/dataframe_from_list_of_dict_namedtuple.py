"""
Create a DataFrame from a list of dicts using NamedTuple rows.
"""

from collections import namedtuple

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    Activity = namedtuple("Activity", ["type_activity_id", "type_activity_name"])

    records = [
        Activity(type_activity_id=1, type_activity_name="Cycling"),
        Activity(type_activity_id=2, type_activity_name="Running"),
        Activity(type_activity_id=3, type_activity_name="Swimming"),
    ]

    df = spark.createDataFrame(records)
    df.printSchema()
    df.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("creation-namedtuple")
    main(spark)
    spark.stop()
