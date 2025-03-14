from pyspark.sql import SparkSession

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

    new_df = df.selectExpr("id",
                           "get_json_object(value, '$.ZipCodeType') as ZipCodeType")
    new_df.show(truncate=False)
