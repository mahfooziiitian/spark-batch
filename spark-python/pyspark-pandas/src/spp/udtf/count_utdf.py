"""Count UDTF — stateful UDTF with ``terminate()``.

Demonstrates a UDTF that accumulates state in ``eval()`` and emits a
final result in ``terminate()``, useful for custom aggregation logic.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf

from spp.session import create_spark_session


@udtf(returnType="cnt: int")
class CountUDTF:
    def __init__(self):
        self.count = 0

    def eval(self, x: int):
        self.count += 1

    def terminate(self):
        yield (self.count,)


def main(spark: SparkSession) -> None:
    spark.udtf.register("count_udtf", CountUDTF)

    print("=== Single partition (1 worker) ===")
    spark.sql("SELECT * FROM range(0, 10, 1, 1), LATERAL count_udtf(id)").show()

    print("=== Two partitions (2 workers) ===")
    spark.sql("SELECT * FROM range(0, 10, 1, 2), LATERAL count_udtf(id)").show()


if __name__ == "__main__":
    spark = create_spark_session("count-udtf")
    main(spark)
    spark.stop()
