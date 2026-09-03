"""Read nested XML with the built-in xml source and flatten it iteratively."""

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, schema_of_xml
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructType,
)

from spark_xml.util.sample_data import ensure_nested_batch_xml


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
                fieldNames = list(filter(lambda elem: elem != field.name, fieldNames))
                fieldNames.extend(struct_fields)
                # print(fieldNames)
                df = df.selectExpr(*fieldNames)
                # df.printSchema()
        loop += 1
    return df


if __name__ == "__main__":
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xmlFile = str(ensure_nested_batch_xml(data_home / "file_data" / "xml" / "nested_xml.xml"))

    spark = SparkSession.builder.master("local[*]").appName("spark-db-xml").getOrCreate()

    issuance = (
        spark.read.format("xml")
        .option("rootTag", "DWHBatch")
        .option("rowTag", "DWHBatch")
        .option("excludeAttribute", True)
        .
        # schema(customSchema).
        load(xmlFile)
    )

    # print(issuance.schema)
    issuance.printSchema()
    print(issuance.schema.simpleString())

    payloadSchema = issuance.select(
        schema_of_xml(lit(issuance.select(col("Records.Issuance").cast(StringType())).first()[0]))
    ).first()[0]
    print(payloadSchema)

    # issuance.show()
    issuance.select(
        "Header.BatchId",
        "Header.TotalNoOfRecords",
        col("Records.Issuance").cast(StringType()),
    ).show()
    # print(flatteningIterative(issuance).count())
