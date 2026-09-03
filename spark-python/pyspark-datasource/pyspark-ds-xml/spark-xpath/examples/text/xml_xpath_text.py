"""Extract repeated text nodes from an XML column with Spark's ``xpath`` function.

``xpath(col, expr)`` returns an **array** of strings for every node the
expression matches — here every ``<b>`` under ``<a>``. Run it directly:

    uv run python examples/text/xml_xpath_text.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("xml_xpath_text").master(os.environ.get("SPARK_MASTER", "local[*]")).getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame([("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>",)], ["x"])

    # xpath() -> array<string> of every matching text node.
    result = df.select(
        F.xpath(df.x, F.lit("a/b/text()")).alias("b_values"),
        F.xpath(df.x, F.lit("a/c/text()")).alias("c_values"),
    )
    result.show(truncate=False)

    spark.stop()
