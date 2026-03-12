from pyspark.sql import functions as f
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import _parse_datatype_json_string

from spark_xml.util.session.spark_session_util import get_spark_session


def ext_from_xml(spark, xml_column, schema, options={}):
    java_column = _to_java_column(xml_column.cast("string"))
    java_schema = spark._jsparkSession.parseDataType(schema.json())
    scala_map = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(options)
    jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(
        java_column, java_schema, scala_map
    )
    return Column(jc)


def ext_schema_of_xml_df(spark, df, options={}):

    assert len(df.columns) == 1

    scala_options = spark._jvm.PythonUtils.toScalaMap(options)
    java_xml_module = getattr(
        getattr(spark._jvm.com.databricks.spark.xml, "package$"), "MODULE$"
    )
    java_schema = java_xml_module.schema_of_xml_df(df._jdf, scala_options)
    return _parse_datatype_json_string(java_schema.json())


if __name__ == "__main__":
    spark = get_spark_session(
        app_name="spark-xml-array-of-structs",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )
    xml = [
        (
            "url",
            """<Level_0 Id0="Id0_value_file1">
        <Level_1 Id1_1 ="Id3_value" Id_2="Id2_value">
          <Level_2_A>A</Level_2_A>
          <Level_2>
            <Level_3>
              <Level_4>
                <Date>2021-01-01</Date>
                <Value>4_1</Value>
              </Level_4>
              <Level_4>
                <Date>2021-01-02</Date>
                <Value>4_2</Value>
              </Level_4>
            </Level_3>
          </Level_2>
        </Level_1>
      </Level_0>""",
        ),
        (
            "url",
            """<Level_0 Id0="Id0_value_file2">
        <Level_1 Id1_1 ="Id3_value" Id_2="Id2_value">
          <Level_2_A>A</Level_2_A>
          <Level_2>
            <Level_3>
              <Level_4>
                <Date>2021-01-01</Date>
                <Value>4_1</Value>
              </Level_4>
              <Level_4>
                <Date>2021-01-02</Date>
                <Value>4_2</Value>
              </Level_4>
            </Level_3>
          </Level_2>
        </Level_1>
      </Level_0>""",
        ),
    ]

    rdd = spark.sparkContext.parallelize(xml)
    df = spark.createDataFrame(rdd, "url string, content string")

    # XML schema
    payloadSchema = ext_schema_of_xml_df(spark, df.select("content"))

    # parse xml
    parsed = df.withColumn(
        "parsed", ext_from_xml(spark, df.content, payloadSchema, {"rowTag": "Level_0"})
    )

    parsed.printSchema()

    # select required data
    df2 = parsed.select(
        "parsed._Id0",
        f.explode_outer("parsed.Level_1.Level_2.Level_3.Level_4").alias("Level_4"),
    )
    df2.printSchema()

    df3 = df2.select("_Id0", "Level_4.*")

    df3.printSchema()
    df3.show(truncate=False)
    spark.stop()
