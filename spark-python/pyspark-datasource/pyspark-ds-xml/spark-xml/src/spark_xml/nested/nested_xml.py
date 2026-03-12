import os

from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spark_xml.util.session.spark_session_util import get_spark_session


def flattening_iterative(dataframe: DataFrame):
    df: DataFrame = dataframe
    # df.printSchema()
    flag = True
    loop = 0
    while flag:
        flag = False
        print(f"loop = {loop}")
        for field in df.schema.fields:
            field_names = list(map(lambda x: x.name, df.schema.fields))
            # print(fieldNames)
            if isinstance(field.dataType, ArrayType):
                explode_column = f"explode_outer( {field.name} ) as {field.name}"
                flag = True
                field_names = list(filter(lambda elem: elem != field.name, field_names))
                field_names.append(explode_column)
                df = df.selectExpr(*field_names)
                # print(f"Field name:\n\t{field.name}")
                # df.printSchema()
            elif isinstance(field.dataType, StructType):
                flag = True
                # print(f"{field.name} \n: {field.dataType.fieldNames()}")
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
                field_names = list(filter(lambda elem: elem != field.name, field_names))
                field_names.extend(struct_fields)
                # print(fieldNames)
                df = df.selectExpr(*field_names)
                # df.printSchema()
        loop += 1
    return df


if __name__ == "__main__":
    data_path = (
        Path(os.environ["DATA_HOME"]) / "FileData" / "xml" / "nested_xml.xml"
    )
    xmlFile = data_path.as_posix()

    spark = get_spark_session(
        app_name="spark-xml-array-of-structs",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )


    customSchema = StructType(
        [
            StructField(
                "Header",
                StructType(
                    [
                        StructField("BatchId", StringType(), nullable=True),
                        StructField("TotalNoOfRecords", IntegerType(), nullable=True),
                    ]
                ),
                nullable=True,
            ),
            StructField(
                "Records",
                StructType(
                    [
                        StructField(
                            "Issuance",
                            ArrayType(StructType([StructField("Entry", StringType())])),
                            nullable=True,
                        ),
                        StructField("PolicyChange", StringType(), nullable=True),
                        StructField("Cancellation", StringType(), nullable=True),
                        StructField("Submission", StringType(), nullable=True),
                        StructField("Reinstatement", StringType(), nullable=True),
                        StructField("Rewrite", StringType(), nullable=True),
                        StructField("Renewal", StringType(), nullable=True),
                        StructField("RenewalSubmission", StringType(), nullable=True),
                    ]
                ),
                nullable=True,
            ),
        ]
    )

    # schema = "struct<Header:struct<BatchId:string,TotalNoOfRecords:int>,
    # Records:struct<Issuance:array<Entry:string>," \ "PolicyChange:string,Cancellation:string,Submission:string,
    # Reinstatement:string,Rewrite:string," \ "Renewal:string,RenewalSubmission:string>>"

    print(customSchema.simpleString())

    issuance = (
        spark.read.format("com.databricks.spark.xml")
        .
        # option("rootTag", "DWHBatch").
        # option("rowTag", "Issuance").
        option("excludeAttribute", True)
        .schema(customSchema)
        .load(xmlFile)
    )

    # print(issuance.schema)
    issuance.printSchema()

    # issuance.show()
    issuance.select(
        "Header.BatchId", "Header.TotalNoOfRecords", "Records.Issuance"
    ).show()
    # print(flatteningIterative(issuance).count())
