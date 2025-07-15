import os
import sys
import xml.etree.ElementTree as Et

from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import _parse_datatype_json_string

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_8"]
os.environ["PYSPARK_PYTHON"] = sys.executable


def ext_from_xml(spark: SparkSession, xml_column, schema, options=None):
    if options is None:
        options = {}
    java_column = _to_java_column(xml_column.cast("string"))
    java_schema = spark._jsparkSession.parseDataType(schema.json())
    scala_map = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(options)
    jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(
        java_column, java_schema, scala_map
    )
    return Column(jc)


def ext_schema_of_xml_df(spark, df, options=None):
    if options is None:
        options = {}
    assert len(df.columns) == 1

    scala_options = spark._jvm.PythonUtils.toScalaMap(options)
    java_xml_module = getattr(
        getattr(spark._jvm.com.databricks.spark.xml, "package$"), "MODULE$"
    )
    java_schema = java_xml_module.schema_of_xml_df(df._jdf, scala_options)
    return _parse_datatype_json_string(java_schema.json())


if __name__ == "__main__":
    # Create a Spark session
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
        .appName("spark-db-xml")
        .getOrCreate()
    )

    # Sample list of strings
    import requests

    data_home = os.environ.get("DATA_HOME", "/tmp/data")
    ca_path = f"{data_home}/certificates/zscaler_root_ca.crt"
    url = "https://sample-files.com/downloads/data/xml/basic-structure.xml"
    data = requests.get(url=url, verify=ca_path).text

    data_xml = Et.fromstring(data)
    xml_data_no_header = Et.tostring(element=data_xml, xml_declaration=False).decode()
    print(xml_data_no_header)

    # Create a DataFrame from the list of strings
    df = spark.createDataFrame([data], "string").toDF("value")

    # Show the DataFrame
    df.show(truncate=False)

    data_path = f"{os.environ['DATA_HOME']}/file_data/text/xml_api_collection"

    df.write.mode("overwrite").text(data_path)

    ds_option = {"multiLine": "true", "rowTag": "person"}

    spark.read.format("xml").options(**ds_option).load(data_path).show(truncate=False)
