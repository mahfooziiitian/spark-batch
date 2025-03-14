from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import MapType, StringType

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName('dataframe_column_json_string')
             .getOrCreate()
             )

    jsonString = """{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
    df = spark.createDataFrame([(1, jsonString)], ["id", "value"])
    df.show(truncate=False)

    schema = MapType(StringType(), StringType(), True)
    df = df.withColumn("value", from_json(df.value, schema=schema))
    df.printSchema()
    df.show(truncate=False)
