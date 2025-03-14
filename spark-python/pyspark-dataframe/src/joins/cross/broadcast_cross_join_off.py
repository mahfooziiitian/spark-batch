from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = (SparkSession.builder
             .appName("cross_join")
             .master("local[*]")
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")
             .getOrCreate())

    df1 = spark.range(1000).withColumnRenamed("id", "num1")
    df2 = spark.range(2000).withColumnRenamed("id", "num2")

    print(f"df1={df1.rdd.getNumPartitions()}")
    print(f"df2={df2.rdd.getNumPartitions()}")

    df3 = df1.crossJoin(df2)

    df3.explain()

    print(f"df3={df3.rdd.getNumPartitions()}")

