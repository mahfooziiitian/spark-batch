"""Parse XML elements with ElementTree and extract a single field via UDF."""

import os
import xml.etree.ElementTree as ET

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

SAMPLE_XML = """\
<CATALOG>
  <CD>
    <TITLE>Empire Burlesque</TITLE>
    <ARTIST>Bob Dylan</ARTIST>
    <COUNTRY>USA</COUNTRY>
    <PRICE>10.90</PRICE>
    <YEAR>1985</YEAR>
  </CD>
  <CD>
    <TITLE>Hide your heart</TITLE>
    <ARTIST>Bonnie Tyler</ARTIST>
    <COUNTRY>UK</COUNTRY>
    <PRICE>9.90</PRICE>
    <YEAR>1988</YEAR>
  </CD>
  <CD>
    <TITLE>Greatest Hits</TITLE>
    <ARTIST>Dolly Parton</ARTIST>
    <COUNTRY>USA</COUNTRY>
    <PRICE>9.90</PRICE>
    <YEAR>1982</YEAR>
  </CD>
  <CD>
    <TITLE>Still got the blues</TITLE>
    <ARTIST>Gary Moore</ARTIST>
    <COUNTRY>UK</COUNTRY>
    <PRICE>10.20</PRICE>
    <YEAR>1990</YEAR>
  </CD>
  <CD>
    <TITLE>Eros</TITLE>
    <ARTIST>Eros Ramazzotti</ARTIST>
    <COUNTRY>EU</COUNTRY>
    <PRICE>9.90</PRICE>
    <YEAR>1997</YEAR>
  </CD>
</CATALOG>
"""


def extract_title(payload: str) -> str | None:
    """Extract the TITLE text from a single CD XML element."""
    doc = ET.fromstring(payload)
    titles = [e.text for e in doc.findall("TITLE") if isinstance(e, ET.Element)]
    return next(iter(titles), None)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("xml-etree-single-field")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    catalog = ET.fromstring(SAMPLE_XML)
    cd_strings = [ET.tostring(cd, encoding="unicode").strip() for cd in catalog.findall("CD")]

    rows = [Row(index=i, cd=xml) for i, xml in enumerate(cd_strings)]
    cd_df = spark.createDataFrame(rows)

    cd_df.show(truncate=False)

    extract_title_udf = udf(extract_title, StringType())

    (cd_df.select("index", extract_title_udf(F.col("cd")).alias("title")).show(truncate=False))

    spark.stop()
