import sys
import numpy as np
from pyspark.sql import *

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("Data Frame Example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    filename = sys.argv[1]
    df = spark.read.csv(filename, inferSchema=True, header=True)
    df.write.format("parquet").mode("overwrite").save("result.parquet")

    df.printSchema()
    df.show(2)

    from pyspark.sql.functions import rand, randn
    df = spark.sqlContext.range(0, 7)
    df.show()

    df.select("id", rand(seed=10).alias("uniform"), randn(seed=27).alias("normal")).show()
    df.describe('uniform', 'normal').show()

    from pyspark.sql.functions import rand
    df = spark.sqlContext.range(0, 10).withColumn('rand1', rand(seed=10)).withColumn('rand2', rand(seed=27))

    df.stat.cov('rand1', 'rand2')
    df.stat.corr('rand1', 'rand2')


    df.columns
    df.describe('DEST_COUNTRY_NAME').show()
    np.random.randint(1000)
