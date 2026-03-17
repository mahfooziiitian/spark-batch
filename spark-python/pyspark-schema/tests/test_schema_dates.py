import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

STRING_SCHEMA = StructType([
    StructField("id",         LongType(),   nullable=False),
    StructField("event",      StringType(), nullable=True),
    StructField("event_date", StringType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
])

RAW_DATA = [
    (1, "signup",   "2024-01-15", "2024-01-15 08:30:00"),
    (2, "login",    "2024-02-20", "2024-02-20 14:00:00"),
    (3, "purchase", "2024-03-10", "2024-03-10 10:15:30"),
    (4, "logout",   "2024-03-10", "2024-03-10 18:45:00"),
]


def build_parsed_df(spark):
    return (spark.createDataFrame(RAW_DATA, schema=STRING_SCHEMA)
            .withColumn("event_date", F.to_date("event_date",   "yyyy-MM-dd"))
            .withColumn("created_at", F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss")))


class TestDateTimestampTypes:
    def test_date_type_after_parsing(self, spark):
        df = build_parsed_df(spark)
        assert isinstance(df.schema["event_date"].dataType, DateType)

    def test_timestamp_type_after_parsing(self, spark):
        df = build_parsed_df(spark)
        assert isinstance(df.schema["created_at"].dataType, TimestampType)

    def test_row_count_preserved(self, spark):
        df = build_parsed_df(spark)
        assert df.count() == 4

    def test_no_null_dates(self, spark):
        df = build_parsed_df(spark)
        assert df.filter(F.col("event_date").isNull()).count() == 0

    def test_no_null_timestamps(self, spark):
        df = build_parsed_df(spark)
        assert df.filter(F.col("created_at").isNull()).count() == 0


class TestDateArithmetic:
    def test_year_extraction(self, spark):
        df = build_parsed_df(spark)
        result = df.withColumn("yr", F.year("event_date"))
        years = {r["yr"] for r in result.select("yr").collect()}
        assert years == {2024}

    def test_month_extraction(self, spark):
        df = build_parsed_df(spark)
        months = (df.withColumn("m", F.month("event_date"))
                    .select("m")
                    .distinct()
                    .orderBy("m")
                    .collect())
        assert [r["m"] for r in months] == [1, 2, 3]

    def test_datediff_non_negative(self, spark):
        df = build_parsed_df(spark)
        result = df.withColumn("days", F.datediff(F.current_date(), F.col("event_date")))
        row = result.orderBy("id").first()
        assert row["days"] >= 0

    def test_groupby_date(self, spark):
        df = build_parsed_df(spark)
        grouped = (df.groupBy("event_date")
                     .agg(F.count("*").alias("cnt")))
        # 2024-03-10 has two events
        march10 = (grouped
                   .filter(F.col("event_date") == F.to_date(F.lit("2024-03-10"), "yyyy-MM-dd"))
                   .first())
        assert march10["cnt"] == 2


class TestTimestampArithmetic:
    def test_hour_extraction(self, spark):
        df = build_parsed_df(spark)
        result = df.withColumn("hr", F.hour("created_at"))
        hours = {r["hr"] for r in result.select("hr").collect()}
        assert hours == {8, 14, 10, 18}

    def test_unix_timestamp_positive(self, spark):
        df = build_parsed_df(spark)
        result = df.withColumn("epoch", F.unix_timestamp("created_at"))
        epochs = [r["epoch"] for r in result.select("epoch").collect()]
        assert all(e > 0 for e in epochs)

    def test_date_trunc_to_hour(self, spark):
        df = build_parsed_df(spark)
        result = df.withColumn("trunc", F.date_trunc("hour", "created_at"))
        row = result.filter(F.col("id") == 1).first()
        # Minutes and seconds should be zeroed
        assert row["trunc"].minute == 0
        assert row["trunc"].second == 0


class TestDateTimestampSchema:
    def test_date_simple_string(self):
        assert DateType().simpleString() == "date"

    def test_timestamp_simple_string(self):
        assert TimestampType().simpleString() == "timestamp"

    def test_date_type_name(self):
        assert DateType().typeName() == "date"

    def test_timestamp_type_name(self):
        assert TimestampType().typeName() == "timestamp"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
