from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("xml_xpath_text").getOrCreate()
    df = spark.createDataFrame(
        [('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>',)], ['x'])
    df.select(xpath(df.x, lit('a/b/text()')).alias('r')).collect()
