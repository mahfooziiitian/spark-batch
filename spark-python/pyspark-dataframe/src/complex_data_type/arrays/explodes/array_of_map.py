from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StringType, ArrayType, MapType
from pyspark.sql.functions import col, explode

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("SparkByExamples.com")
             .master("local[1]")
             .getOrCreate())

    arrayMapSchema = (
        StructType()
        .add("name", StringType())
        .add("properties", ArrayType(MapType(StringType(), StringType(), True)))
    )

    arrayMapData = [
        Row("James", [{"hair": "black", "eye": "brown"}, {"height": "5.9"}]),
        Row("Michael", [{"hair": "brown", "eye": "black"}, {"height": "6"}]),
        Row("Robert", [{"hair": "red", "eye": "gray"}, {"height": "6.3"}])
    ]

    df = spark.createDataFrame(spark.sparkContext.parallelize(arrayMapData), arrayMapSchema)
    df.printSchema()
    df.show(truncate=False)

    df2 = df.select("name", explode(col("properties")))
    df2.printSchema()
    df2.show(truncate=False)

# end main
