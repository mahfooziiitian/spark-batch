"""Arrow-optimized UDTF.

Demonstrates a UDTF with ``useArrow=True`` for faster data transfer
between the JVM and Python workers.  Arrow UDTFs are ideal when the
UDTF produces many rows.

Usage::

    from spp.udtf.arrow_udtf import PlusOne
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, udtf

from spp.session import create_spark_session


@udtf(returnType="input: int, output: int", useArrow=True)
class PlusOne:
    """Yield (x, x+1) for the input — Arrow-optimized."""

    def eval(self, x: int):
        yield x, x + 1


@udtf(returnType="n: int, factorial: long", useArrow=True)
class Factorial:
    """Yield (i, i!) for i in [1, n] — Arrow-optimized."""

    def eval(self, n: int):
        result = 1
        for i in range(1, n + 1):
            result *= i
            yield i, result


def main(spark: SparkSession) -> None:
    print("=== PlusOne (Arrow UDTF) ===")
    PlusOne(lit(42)).show()

    print("=== Factorial (Arrow UDTF) ===")
    Factorial(lit(10)).show()

    # Register for SQL
    spark.udtf.register("plus_one", PlusOne)
    spark.udtf.register("factorial", Factorial)

    print("=== SQL: LATERAL plus_one ===")
    spark.sql("SELECT id, o.* FROM range(5), LATERAL plus_one(id) o").show()

    print("=== SQL: factorial(6) ===")
    spark.sql("SELECT * FROM factorial(6)").show()


if __name__ == "__main__":
    spark = create_spark_session("arrow-udtf")
    main(spark)
    spark.stop()
