from multiprocessing.pool import ThreadPool


def get_and_save_table_in_pyspark(table_name):
    import os.path
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("sparkNetsuite").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")

    jdbc_df = spark.read \
        .format("jdbc") \
        .option("url", "OURCONNECTIONURL;") \
        .option("driver", "com.netsuite.jdbc.openaccess.OpenAccessDriver") \
        .option("dbtable", table_name) \
        .option("user", "USERNAME") \
        .option("password", "PASSWORD") \
        .load()

    file_path = "C:\\src\\NetsuiteSparkProject\\" + table_name + "\\" + table_name + ".parquet"
    jdbc_df.write.parquet(file_path)
    file_exists = os.path.exists(file_path)
    if file_exists:
        return file_path + " exists!"
    else:
        return file_path + " could not be written!"


if __name__ == '__main__':
    pool = ThreadPool(5)
    top5Tables = []
    results = pool.map(get_and_save_table_in_pyspark, top5Tables)
    pool.close()
    pool.join()
    print(*results, sep='\n')
