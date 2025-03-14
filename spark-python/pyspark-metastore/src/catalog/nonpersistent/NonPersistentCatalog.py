if __name__ == '__main__':
    from pyspark.sql import SparkSession

    spark_session = SparkSession.builder.getOrCreate()
    # input_df = get_some_df(spark_session)
    # input_df.createTempView('raw_electricity_data')
