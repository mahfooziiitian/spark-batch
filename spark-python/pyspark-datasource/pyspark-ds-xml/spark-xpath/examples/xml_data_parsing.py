"""Parse a flat XML message column using Spark's XPath SQL functions.

Loads two ``<Msg>`` documents as a single-column DataFrame, registers a temp
view, and pulls values out with ``xpath_string`` / ``xpath``. Self-contained —
the sample data is embedded inline. Run it directly:

    uv run python examples/xml_data_parsing.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("xml_data_parsing")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    data = [
        "<Msg><Header><tag1>some str1</tag1><tag2>2</tag2><tag3>2022-02-16 "
        "10:39:26.730</tag3></Header><Body><Pair><N>N1</N><V>V1</V></Pair>"
        "<Pair><N>N2</N><V>V2</V></Pair><Pair><N>N3</N><V>V3</V></Pair></Body></Msg>",
        "<Msg><Header><tag1>some str2</tag1><tag2>5</tag2><tag3>2022-02-17 "
        "10:39:26.730</tag3></Header><Body><Pair><N>N4</N><V>V4</V></Pair>"
        "<Pair><N>N5</N><V>V5</V></Pair></Body></Msg>",
    ]

    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("xml_df")
    df.printSchema()
    df.show(truncate=False)

    # xpath_string -> concatenated text of matched nodes;
    # xpath -> array<string> of every matched node.
    spark.sql("""
        SELECT
            xpath_string(data, 'Msg/Header/tag1')   AS tag1,
            xpath_string(data, 'Msg/Header/tag2')   AS tag2,
            xpath(data, 'Msg/Body/Pair/N/text()')   AS names,
            xpath(data, 'Msg/Body/Pair/V/text()')   AS values
        FROM xml_df
        """).show(truncate=False)

    spark.stop()
