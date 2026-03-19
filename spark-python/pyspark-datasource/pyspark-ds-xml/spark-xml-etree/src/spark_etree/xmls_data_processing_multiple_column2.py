"""Extract XML attributes and nested records, then explode arrays into rows."""

import os
import xml.etree.ElementTree as ET

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, IntegerType, StringType


@udf(returnType=ArrayType(StringType()))
def extract_attributes(xml: str) -> list[str]:
    """Return the 'a' and 'b' attributes from the root element."""
    doc = ET.fromstring(xml)
    return [doc.attrib["a"], doc.attrib["b"]]


def extract_record_ids(xml: str) -> list[int]:
    """Return all record/@id values as integers."""
    doc = ET.fromstring(xml)
    return [int(r.attrib["id"]) for r in doc.findall("records/record")]


SAMPLE_DATA = [
    {
        "id": 1,
        "data": '<test a="100" b="200">  <records>    <record id="101" />    <record id="201" />  </records></test>',
    },
    {
        "id": 2,
        "data": '<test a="200" b="400">  <records>    <record id="202" />    <record id="402" />  </records></test>',
    },
    {
        "id": 3,
        "data": '<test a="300" b="600">'
        "  <records>"
        '    <record id="303" />'
        '    <record id="603" />'
        '    <record id="903" />'
        "  </records>"
        "</test>",
    },
]


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("xml-etree-attributes-explode")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(SAMPLE_DATA)
    df.printSchema()
    df.show(truncate=False)

    # Extract root-level attributes
    df_attrs = df.withColumn("attrs", extract_attributes(F.col("data")))
    df_attrs.show(truncate=False)

    # Extract nested record IDs and explode into individual rows
    extract_record_ids_udf = udf(extract_record_ids, ArrayType(IntegerType()))

    df_exploded = (
        df.withColumn("record_ids", extract_record_ids_udf(F.col("data")))
        .withColumn("record_id", F.explode("record_ids"))
        .select("id", "record_id")
    )
    df_exploded.show(truncate=False)

    spark.stop()
