"""
Returns multiple JSON objects as a tuple.

jsonStr: A STRING expression with well-formed JSON.
pathN: A STRING literal with a JSON path expression.

A single row composed of the JSON objects.
If any object cannot be found, NULL is returned for that object.

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, json_tuple

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName('dataframe_to_json')
             .getOrCreate()
             )

    jsonString = """{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
    df = spark.createDataFrame([(1, jsonString)], ["id", "value"])
    df.printSchema()
    df.show(truncate=False)

    df_tuple = (df.select(col("id"), json_tuple("value", "Zipcode", "ZipCodeType", "City")) \
                .toDF("id", "Zipcode", "ZipCodeType", "City"))

    df_tuple.printSchema()
    df_tuple.show(truncate=False)
