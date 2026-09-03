"""Read a whole XML file and query it with Spark's XPath SQL functions.

Demonstrates reading an XML document from disk as a single row
(``spark.read.text(..., wholetext=True)``) and extracting header/record fields
with ``xpath_string`` and ``xpath``.

Self-contained: a sample document is written to ``XML_INPUT`` (defaults to
``/tmp/nested_xml.xml``) if it does not already exist. Run it directly:

    uv run python examples/netsted/nested_xml_xpath.py

Override the input file with an environment variable:

    XML_INPUT=/path/to/nested_xml.xml uv run python examples/netsted/nested_xml_xpath.py
"""

import os

from pyspark.sql import SparkSession

SAMPLE_XML = """<DWHBatch>
  <Header>
    <BatchId>B-1001</BatchId>
    <TotalNoOfRecords>2</TotalNoOfRecords>
  </Header>
  <Records>
    <Issuance>
      <Entry><PolicyId>P-1</PolicyId><Premium>1200</Premium></Entry>
      <Entry><PolicyId>P-2</PolicyId><Premium>1850</Premium></Entry>
    </Issuance>
  </Records>
</DWHBatch>
"""

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("nested_xml_xpath")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    xml_file = os.environ.get("XML_INPUT", "/tmp/nested_xml.xml")
    if not os.path.exists(xml_file):
        with open(xml_file, "w", encoding="utf-8") as handle:
            handle.write(SAMPLE_XML)

    # wholetext=True keeps the entire document in a single "value" row.
    df = spark.read.text(paths=xml_file, wholetext=True)
    df.createOrReplaceTempView("policy_center")

    spark.sql("""
        SELECT
            xpath_string(value, 'DWHBatch/Header/BatchId')          AS batchId,
            xpath_string(value, 'DWHBatch/Header/TotalNoOfRecords') AS totalNoOfRecords,
            xpath(value, 'DWHBatch/Records/Issuance/Entry/PolicyId/text()') AS policyIds,
            xpath(value, 'DWHBatch/Records/Issuance/Entry/Premium/text()')  AS premiums
        FROM policy_center
        """).show(truncate=False)

    spark.stop()
