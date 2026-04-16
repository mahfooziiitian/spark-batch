"""Date Expander UDTF — generate a row per day in a date range.

Demonstrates a practical UDTF that expands a (start, end) date pair
into one row per calendar day.
"""

from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, udtf

from spp.session import create_spark_session


@udtf(returnType="date: string")
class DateExpander:
    def eval(self, start_date: str, end_date: str):
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        while current <= end:
            yield (current.strftime("%Y-%m-%d"),)
            current += timedelta(days=1)


def main(spark: SparkSession) -> None:
    print("=== Expand date range ===")
    DateExpander(lit("2024-01-28"), lit("2024-02-02")).show()

    spark.udtf.register("expand_dates", DateExpander)
    print("=== SQL usage with LATERAL ===")
    spark.sql("""
        SELECT r.range_name, d.date
        FROM VALUES ('Q1-start', '2024-01-01', '2024-01-03'),
                    ('Q2-start', '2024-04-01', '2024-04-03') AS r(range_name, s, e),
        LATERAL expand_dates(r.s, r.e) d
    """).show()


if __name__ == "__main__":
    spark = create_spark_session("date-expander-udtf")
    main(spark)
    spark.stop()
