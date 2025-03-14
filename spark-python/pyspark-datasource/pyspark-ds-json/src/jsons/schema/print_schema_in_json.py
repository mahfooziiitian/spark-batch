from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Create SparkSession
    spark = (SparkSession.builder.master("local[*]")
             .appName('print_schema_in_json')
             .getOrCreate())

    # Create DataFrame
    columns = ["language", "fee"]
    data = [("Java", 20000), ("Python", 10000), ("Scala", 10000)]

    df = spark.createDataFrame(data).toDF(*columns)
    df.printSchema()

    schemaString = df.schema.simpleString()
    print(schemaString)

    print(df.schema.json())
