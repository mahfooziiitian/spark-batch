import pytest
from pyspark.sql import functions as F


class SquareNumbers:
    """Yields (num, num²) for every integer in [start, end]."""

    def eval(self, start: int, end: int):
        for num in range(start, end + 1):
            yield (num, num * num)


class FibonacciNumbers:
    """Yields the first *n* Fibonacci numbers plus a sentinel row."""

    def __init__(self):
        self._count = 0

    def eval(self, n: int):
        a, b = 0, 1
        for _ in range(n):
            yield (self._count, a)
            a, b = b, a + b
            self._count += 1

    def terminate(self):
        yield (self._count, -1)


class TestBasicUDTF:
    def test_square_numbers(self, spark):
        spark.sparkContext.addPyFile(__file__)
        square = F.udtf(SquareNumbers, returnType="num: int, squared: int")
        result = square(F.lit(1), F.lit(4))
        assert result.count() == 4
        row = result.filter(F.col("num") == 3).first()
        assert row["squared"] == 9

    def test_square_single_value(self, spark):
        spark.sparkContext.addPyFile(__file__)
        square = F.udtf(SquareNumbers, returnType="num: int, squared: int")
        result = square(F.lit(5), F.lit(5))
        assert result.count() == 1
        assert result.first()["squared"] == 25


class TestLifecycleUDTF:
    def test_fibonacci(self, spark):
        spark.sparkContext.addPyFile(__file__)
        fib = F.udtf(FibonacciNumbers, returnType="idx: int, value: int")
        result = fib(F.lit(5))
        # 5 Fibonacci values + 1 terminate sentinel = 6 rows
        assert result.count() == 6

    def test_fibonacci_values(self, spark):
        spark.sparkContext.addPyFile(__file__)
        fib = F.udtf(FibonacciNumbers, returnType="idx: int, value: int")
        rows = fib(F.lit(5)).orderBy("idx").collect()
        values = [r["value"] for r in rows[:5]]
        assert values == [0, 1, 1, 2, 3]


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
