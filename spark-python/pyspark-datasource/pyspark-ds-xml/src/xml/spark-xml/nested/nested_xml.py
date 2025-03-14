import os

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType, StringType, StructField, IntegerType, ArrayType
from pyspark.sql.functions import explode, col


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
        .config('spark.jars.packages', 'com.databricks:spark-xml_2.13:0.16.0') \
        .appName("spark-db-xml").getOrCreate()

    customSchema = StructType([
        StructField("Header",
                    StructType(
                        [StructField("BatchId", StringType(), nullable=True),
                         StructField("TotalNoOfRecords", IntegerType(), nullable=True)]
                    ),
                    nullable=True
                    ),
        StructField("Records", StructType(
            [
                StructField("Issuance",
                            ArrayType(StructType([
                                StructField('Entry', StringType())
                            ]))
                            , nullable=True),
                StructField("PolicyChange", StringType(), nullable=True),
                StructField("Cancellation", StringType(), nullable=True),
                StructField("Submission", StringType(), nullable=True),
                StructField("Reinstatement", StringType(), nullable=True),
                StructField("Rewrite", StringType(), nullable=True),
                StructField("Renewal", StringType(), nullable=True),
                StructField("RenewalSubmission", StringType(), nullable=True),
            ]
        ), nullable=True)
    ])

    # schema = "struct<Header:struct<BatchId:string,TotalNoOfRecords:int>,
    # Records:struct<Issuance:array<Entry:string>," \ "PolicyChange:string,Cancellation:string,Submission:string,
    # Reinstatement:string,Rewrite:string," \ "Renewal:string,RenewalSubmission:string>>"

    print(customSchema.simpleString())

    issuance = (spark.read.format("com.databricks.spark.xml").
                # option("rootTag", "DWHBatch").
                # option("rowTag", "Issuance").
                option("excludeAttribute", True).
                schema(customSchema).
                load(xmlFile)
                )

    # print(issuance.schema)
    issuance.printSchema()

    # issuance.show()
    issuance.select("Header.BatchId", "Header.TotalNoOfRecords", "Records.Issuance").show()
    # print(flatteningIterative(issuance).count())
