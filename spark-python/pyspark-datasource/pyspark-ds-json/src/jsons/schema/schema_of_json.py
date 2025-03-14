from pyspark.sql import SparkSession
from pyspark.sql.functions import schema_of_json, lit

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

    schemaStr = spark.range(1) \
        .select(schema_of_json(lit("""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""))) \
        .collect()[0][0]
    print(schemaStr)
