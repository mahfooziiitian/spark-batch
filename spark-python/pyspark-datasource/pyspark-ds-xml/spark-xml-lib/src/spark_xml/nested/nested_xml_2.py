"""Demonstrate reading deeply nested XML using from_xml, schema_of_xml, and
iterative flattening.

Generates sample XML resembling a DWH batch with Header metadata and
nested Issuance records, then applies JVM bridge functions and iterative
struct/array flattening.
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.functions import col
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructType,
    _parse_datatype_json_string,
)

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

SAMPLE_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <DWHBatch>
      <Header>
        <BatchId>BATCH-2024-001</BatchId>
        <BatchDate>2024-06-15</BatchDate>
        <TotalNoOfRecords>4</TotalNoOfRecords>
        <Source>CoreBanking</Source>
      </Header>
      <Records>
        <Issuance>
          <AccountId>ACC-10001</AccountId>
          <Product>
            <ProductCode>HL</ProductCode>
            <ProductName>Home Loan</ProductName>
            <Category>Secured</Category>
          </Product>
          <Customer>
            <CustomerId>CUST-5001</CustomerId>
            <Name>Alice Johnson</Name>
            <Segment>Retail</Segment>
          </Customer>
          <Amount>250000.00</Amount>
          <Currency>USD</Currency>
          <IssuanceDate>2024-01-10</IssuanceDate>
          <Status>Active</Status>
        </Issuance>
        <Issuance>
          <AccountId>ACC-10002</AccountId>
          <Product>
            <ProductCode>PL</ProductCode>
            <ProductName>Personal Loan</ProductName>
            <Category>Unsecured</Category>
          </Product>
          <Customer>
            <CustomerId>CUST-5002</CustomerId>
            <Name>Bob Smith</Name>
            <Segment>Premium</Segment>
          </Customer>
          <Amount>50000.00</Amount>
          <Currency>EUR</Currency>
          <IssuanceDate>2024-02-20</IssuanceDate>
          <Status>Active</Status>
        </Issuance>
        <Issuance>
          <AccountId>ACC-10003</AccountId>
          <Product>
            <ProductCode>CC</ProductCode>
            <ProductName>Credit Card</ProductName>
            <Category>Revolving</Category>
          </Product>
          <Customer>
            <CustomerId>CUST-5003</CustomerId>
            <Name>Carol Lee</Name>
            <Segment>Retail</Segment>
          </Customer>
          <Amount>15000.00</Amount>
          <Currency>GBP</Currency>
          <IssuanceDate>2024-03-05</IssuanceDate>
          <Status>Pending</Status>
        </Issuance>
        <Issuance>
          <AccountId>ACC-10004</AccountId>
          <Product>
            <ProductCode>AL</ProductCode>
            <ProductName>Auto Loan</ProductName>
            <Category>Secured</Category>
          </Product>
          <Customer>
            <CustomerId>CUST-5004</CustomerId>
            <Name>David Chen</Name>
            <Segment>Corporate</Segment>
          </Customer>
          <Amount>35000.00</Amount>
          <Currency>USD</Currency>
          <IssuanceDate>2024-04-18</IssuanceDate>
          <Status>Active</Status>
        </Issuance>
      </Records>
    </DWHBatch>
""")


def generate_sample_xml(file_path: Path) -> None:
    """Write the DWH batch sample XML to *file_path*."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(SAMPLE_XML, encoding="utf-8")
    print(f"Generated sample XML → {file_path}")


def ext_from_xml(spark: SparkSession, xml_column, schema, options={}):
    java_column = _to_java_column(xml_column.cast("string"))
    java_schema = spark._jsparkSession.parseDataType(schema.json())
    scala_map = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(options)
    jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(
        java_column, java_schema, scala_map
    )
    return Column(jc)


def ext_schema_of_xml_df(spark: SparkSession, df, options={}):
    assert len(df.columns) == 1

    scala_options = spark._jvm.PythonUtils.toScalaMap(options)
    java_xml_module = getattr(
        getattr(spark._jvm.com.databricks.spark.xml, "package$"), "MODULE$"
    )
    java_schema = java_xml_module.schema_of_xml_df(df._jdf, scala_options)
    return _parse_datatype_json_string(java_schema.json())


def flattening_iterative(dataframe: DataFrame) -> DataFrame:
    """Recursively explode arrays and expand structs until fully flat."""
    df: DataFrame = dataframe
    flag = True
    loop = 0
    while flag:
        flag = False
        print(f"loop = {loop}")
        for field in df.schema.fields:
            field_names = list(map(lambda x: x.name, df.schema.fields))
            if isinstance(field.dataType, ArrayType):
                explode_column = f"explode_outer( {field.name} ) as {field.name}"
                flag = True
                field_names = list(
                    filter(lambda elem: elem != field.name, field_names)
                )
                field_names.append(explode_column)
                df = df.selectExpr(*field_names)
            elif isinstance(field.dataType, StructType):
                flag = True
                struct_fields = list(
                    map(
                        lambda child_name: field.name
                        + "."
                        + child_name.name
                        + " as "
                        + field.name
                        + "_"
                        + child_name.name,
                        field.dataType.fields,
                    )
                )
                field_names = list(
                    filter(lambda elem: elem != field.name, field_names)
                )
                field_names.extend(struct_fields)
                df = df.selectExpr(*field_names)
        loop += 1
    return df


if __name__ == "__main__":
    data_path = (
        Path(os.environ["DATA_HOME"]) / "FileData" / "xml" / "nested_xml.xml"
    )
    generate_sample_xml(data_path)

    spark = get_spark_session(
        app_name="spark-xml-nested-dwh",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    issuance = (
        spark.read.format("com.databricks.spark.xml")
        .option("rootTag", "DWHBatch")
        .option("rowTag", "DWHBatch")
        .option("excludeAttribute", True)
        .load(data_path.as_posix())
    )

    print("\n=== Raw Schema ===")
    issuance.printSchema()
    print(issuance.schema.simpleString())

    # Infer schema of the nested Issuance XML string
    payload_schema = ext_schema_of_xml_df(
        spark, issuance.select(col("Records.Issuance").cast(StringType()))
    )
    print(f"\n=== Issuance payload schema ===\n{payload_schema}")

    print("\n=== Header + Issuance (as string) ===")
    issuance.select(
        "Header.BatchId",
        "Header.TotalNoOfRecords",
        col("Records.Issuance").cast(StringType()),
    ).show(truncate=False)

    print("\n=== Iterative Flattening ===")
    flat_df = flattening_iterative(issuance)
    flat_df.printSchema()
    flat_df.show(truncate=False)

    spark.stop()
