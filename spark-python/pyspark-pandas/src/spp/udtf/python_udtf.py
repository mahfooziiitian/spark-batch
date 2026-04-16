"""Python UDTFs — basic usage.

Demonstrates creating UDTFs via the ``@udtf`` decorator and the
``udtf()`` function, then invoking them from PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, udtf

from spp.session import create_spark_session


class SquareNumbers:
    """Yields (num, num²) for each integer in [start, end]."""

    def eval(self, start: int, end: int):
        for num in range(start, end + 1):
            yield num, num * num


def main(spark: SparkSession) -> None:
    square_num = udtf(SquareNumbers, returnType="num: int, squared: int")
    print("=== udtf() function registration ===")
    square_num(lit(1), lit(5)).show()

    @udtf(returnType="num: int, cubed: int")
    class CubeNumbers:
        def eval(self, start: int, end: int):
            for num in range(start, end + 1):
                yield num, num**3

    print("=== @udtf decorator ===")
    CubeNumbers(lit(1), lit(5)).show()


if __name__ == "__main__":
    spark = create_spark_session("python-udtf-basic")
    main(spark)
    spark.stop()
