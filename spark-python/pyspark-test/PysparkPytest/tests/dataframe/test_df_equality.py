from pyspark.testing.utils import assertDataFrameEqual


def test_df_equality(spark):
    df1 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
    df2 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
    assertDataFrameEqual(df1, df2)
