"""UDTF with table argument — process entire table rows.

Demonstrates a UDTF that accepts ``Row`` objects from a table argument,
enabling row-level filtering and transformation logic.
"""

from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf

from spp.session import create_spark_session


@udtf(returnType="id: int, label: string")
class FilterAndLabel:
    """Passes through rows where id > threshold, adding a label."""

    def eval(self, row: Row):
        if row["id"] > 5:
            yield row["id"], "above_threshold"


def main(spark: SparkSession) -> None:
    spark.udtf.register("filter_and_label", FilterAndLabel)

    print("=== Table argument with range ===")
    spark.sql("SELECT * FROM filter_and_label(TABLE(SELECT * FROM range(10)))").show()

    print("=== Table argument with custom data ===")
    spark.sql("""
        SELECT * FROM filter_and_label(
            TABLE(SELECT id FROM VALUES (3), (6), (8), (2), (9) AS t(id))
        )
    """).show()


if __name__ == "__main__":
    spark = create_spark_session("udtf-table-argument")
    main(spark)
    spark.stop()
