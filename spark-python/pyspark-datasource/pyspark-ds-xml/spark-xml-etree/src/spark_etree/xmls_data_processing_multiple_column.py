"""Parse XML elements with ElementTree and extract multiple fields via a struct UDF."""

import os
import xml.etree.ElementTree as ET

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructField, StructType

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


def select_text(xml_doc: ET.Element, xpath: str) -> str | None:
    """Return text of the first matching element, or None."""
    nodes = [e.text for e in xml_doc.findall(xpath) if isinstance(e, ET.Element)]
    return next(iter(nodes), None)


def extract_cd_info(payload: str) -> dict[str, str | None]:
    """Extract title, artist, country, and year from a single CD XML element."""
    doc = ET.fromstring(payload)
    return {
        "title": select_text(doc, "TITLE"),
        "artist": select_text(doc, "ARTIST"),
        "country": select_text(doc, "COUNTRY"),
        "year": select_text(doc, "YEAR"),
    }


CD_INFO_SCHEMA = StructType(
    [
        StructField("title", StringType(), True),
        StructField("artist", StringType(), True),
        StructField("country", StringType(), True),
        StructField("year", StringType(), True),
    ]
)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("xml-etree-multi-field")
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

    extract_cd_info_udf = udf(extract_cd_info, CD_INFO_SCHEMA)

    (
        cd_df.withColumn("info", extract_cd_info_udf("cd"))
        .select("index", "info.title", "info.artist", "info.country", "info.year")
        .show(truncate=False)
    )

    spark.stop()
