import os

from pyspark.sql import SparkSession
import xml.etree.ElementTree as Et
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import _parse_datatype_json_string


def ext_from_xml(spark: SparkSession, xml_column, schema, options=None):
    if options is None:
        options = {}
    java_column = _to_java_column(xml_column.cast('string'))
    java_schema = spark._jsparkSession.parseDataType(schema.json())
    scala_map = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(options)
    jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(
        java_column, java_schema, scala_map)
    return Column(jc)


def ext_schema_of_xml_df(spark, df, options=None):
    if options is None:
        options = {}
    assert len(df.columns) == 1

    scala_options = spark._jvm.PythonUtils.toScalaMap(options)
    java_xml_module = getattr(getattr(
        spark._jvm.com.databricks.spark.xml, "package$"), "MODULE$")
    java_schema = java_xml_module.schema_of_xml_df(df._jdf, scala_options)
    return _parse_datatype_json_string(java_schema.json())


if __name__ == '__main__':
    # Create a Spark session
    spark = (SparkSession.builder.master("local[*]")
             .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0')
             .appName("spark-db-xml").getOrCreate())

    # Sample list of strings
    import requests

    url = "https://thetestrequest.com/authors.xml"
    data = requests.get(url).text

    data_xml = Et.fromstring(data)
    xml_data_no_header = Et.tostring(element=data_xml, xml_declaration=False).decode()
    print(xml_data_no_header)

    # Create a DataFrame from the list of strings
    df = spark.createDataFrame([data], "string").toDF("value")

    # Show the DataFrame
    df.show(truncate=False)

    data_path = f"{os.environ['DATA_HOME']}\\FileData\\Text\\xml_api_data"

    df.write.mode("overwrite").text(data_path)

    ds_option = {
        "multiLine": "true",
        "rowTag": "object"
    }

    spark.read.format("xml").options(**ds_option).load(data_path).show(truncate=False)

    # spark.sql(f"select schema_of_xml('{xml_data_no_header}', map('multiLine', 'true'))").show(truncate=False)
