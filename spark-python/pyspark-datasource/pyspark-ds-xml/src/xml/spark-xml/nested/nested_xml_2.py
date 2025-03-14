import os

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.column import _to_java_column, Column
from pyspark.sql.types import ArrayType, StructType, StringType, StructField, IntegerType, ArrayType, LongType, \
    _parse_datatype_json_string
from pyspark.sql.functions import explode, col

def ext_from_xml(spark: SparkSession, xml_column, schema, options={}):
    java_column = _to_java_column(xml_column.cast('string'))
    java_schema = spark._jsparkSession.parseDataType(schema.json())
    scala_map = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(options)
    jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(
        java_column, java_schema, scala_map)
    return Column(jc)

def ext_schema_of_xml_df(spark: SparkSession, df, options={}):
    assert len(df.columns) == 1

    scala_options = spark._jvm.PythonUtils.toScalaMap(options)
    java_xml_module = getattr(getattr(
        spark._jvm.com.databricks.spark.xml, "package$"), "MODULE$")
    java_schema = java_xml_module.schema_of_xml_df(df._jdf, scala_options)
    return _parse_datatype_json_string(java_schema.json())

def flatteningIterative(dataframe: DataFrame):
    df: DataFrame = dataframe
    # df.printSchema()
    flag = True
    loop = 0
    while flag:
        flag = False
        print(f"loop = {loop}")
        for field in df.schema.fields:
            fieldNames = list(map(lambda x: x.name, df.schema.fields))
            # print(fieldNames)
            if isinstance(field.dataType, ArrayType):
                explode_column = f"explode_outer( {field.name} ) as {field.name}"
                flag = True
                fieldNames = list(filter(lambda elem: elem != field.name, fieldNames))
                fieldNames.append(explode_column)
                df = df.selectExpr(*fieldNames)
                # print(f"Field name:\n\t{field.name}")
                # df.printSchema()
            elif isinstance(field.dataType, StructType):
                flag = True
                # print(f"{field.name} \n: {field.dataType.fieldNames()}")
                struct_fields = list(
                    map(lambda
                            child_name: field.name + "." + child_name.name + " as " + field.name + "_" + child_name.name,
                        field.dataType.fields))
                fieldNames = list(filter(lambda elem: elem != field.name, fieldNames))
                fieldNames.extend(struct_fields)
                # print(fieldNames)
                df = df.selectExpr(*fieldNames)
                # df.printSchema()
        loop += 1
    return df


if __name__ == '__main__':
    xmlFile = os.environ["DATA_HOME"] + "\\FileData\\Xml\\nested_xml.xml"

    spark = SparkSession.builder.master("local[*]") \
        .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0') \
        .appName("spark-db-xml").getOrCreate()

    issuance = (spark.read.format("com.databricks.spark.xml").
                option("rootTag", "DWHBatch").
                option("rowTag", "DWHBatch").
                option("excludeAttribute", True).
                # schema(customSchema).
                load(xmlFile)
                )

    # print(issuance.schema)
    issuance.printSchema()
    print(issuance.schema.simpleString())

    payloadSchema = ext_schema_of_xml_df(spark, issuance.select(col("Records.Issuance").cast(StringType())))
    print(payloadSchema)

    # issuance.show()
    issuance.select("Header.BatchId", "Header.TotalNoOfRecords", col("Records.Issuance").cast(StringType())).show()
    # print(flatteningIterative(issuance).count())
