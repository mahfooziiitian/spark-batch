from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("xml_data").getOrCreate()

    data = [
        "<Msg><Header><tag1>some str1</tag1><tag2>2</tag2><tag3>2022-02-16 "
        "10:39:26.730</tag3></Header><Body><Pair><N>N1</N><V>V1</V></Pair><Pair><N>N2</N><V>V2</V></Pair><Pair><N>N3"
        "</N><V>V3</V></Pair></Body></Msg>",
        "<Msg><Header><tag1>some str2</tag1><tag2>5</tag2><tag3>2022-02-17 "
        "10:39:26.730</tag3></Header><Body><Pair><N>N4</N><V>V4</V></Pair><Pair><N>N5</N><V>V5</V></Pair></Body></Msg>"
    ]

    df = spark.createDataFrame(data, StringType()) \
        .withColumnRenamed("value", "data")

    df.createOrReplaceTempView("xml_df")

    df.printSchema()

    df.show(truncate=False)

    spark.sql("select xpath_string(data, \'Msg/Header/*\') from xml_df").show(truncate=False)

    df.printSchema()

