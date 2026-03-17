from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, md5


def test_scd1():
    spark = SparkSession.builder.appName("SCD1Test").getOrCreate()

    # Existing data (target)
    target_df = spark.createDataFrame(
        [
            ("cust1", "Alice", "NY"),
            ("cust2", "Bob", "CA"),
        ],
        ["customer_id", "name", "state"],
    )

    # Incoming data (source)
    source_df = spark.createDataFrame(
        [
            ("cust1", "Alice", "NY"),  # no change
            ("cust2", "Bobby", "CA"),  # name change
            ("cust3", "Charlie", "WA"),  # new insert
        ],
        ["customer_id", "name", "state"],
    )

    # Add hash to detect changes
    def add_hash(df):
        return df.withColumn("row_hash", md5(concat_ws("||", *["name", "state"])))

    source_hashed = add_hash(source_df)
    target_hashed = add_hash(target_df)

    # Simulate merge logic
    merged = (
        source_hashed.alias("src")
        .join(target_hashed.alias("tgt"), on="customer_id", how="outer")
        .selectExpr(
            "coalesce(src.customer_id, tgt.customer_id) as customer_id",
            "coalesce(src.name, tgt.name) as name",
            "coalesce(src.state, tgt.state) as state",
            "coalesce(src.row_hash, tgt.row_hash) as row_hash",
        )
    )
    expected_df = spark.createDataFrame(
        [
            ("cust1", "Alice", "NY"),  # unchanged
            ("cust2", "Bobby", "CA"),  # updated
            ("cust3", "Charlie", "WA"),  # inserted
        ],
        ["customer_id", "name", "state"],
    )

    assert (
        merged.select("customer_id", "name", "state").collect() == expected_df.collect()
    )
