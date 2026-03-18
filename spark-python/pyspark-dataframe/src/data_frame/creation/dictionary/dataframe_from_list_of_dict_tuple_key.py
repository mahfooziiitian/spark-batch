"""
Create a DataFrame from a dict whose keys are tuples and values are nested tuples.
Demonstrates how to flatten composite dict structures into a flat row schema.
"""

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    source = {
        ("aaa", "bbb", "ccc"): ((0.3, 1.2, 1.3, 1.5), 1.4, 1),
        ("kkk", "ggg", "ccc", "sss"): ((0.6, 1.2, 1.7, 1.5), 1.4, 2),
    }

    rows = [(list(k),) + v[0] + v[1:] for k, v in source.items()]

    df = spark.sparkContext.parallelize(rows).toDF(
        ["key", "val_1", "val_2", "val_3", "val_4", "val_5", "val_6"]
    )
    df.printSchema()
    df.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("creation-tuple-key")
    main(spark)
    spark.stop()
