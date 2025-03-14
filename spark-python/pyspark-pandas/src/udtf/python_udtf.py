import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, udtf


class SquareNumbers:
    def eval(self, start: int, end: int):
        for num in range(start, end + 1):
            yield num, num * num


os.environ["PYSPARK_PYTHON"] = "C:\\Users\\mahfo\\.virtualenvs\\PysparkPandas-bh8Y5n2t\\Scripts\\python.exe"

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    # Create a UDTF using the class definition and the `udtf` function.
    square_num = udtf(SquareNumbers, returnType="num: int, squared: int")

    # Invoke the UDTF in PySpark.
    square_num(lit(1), lit(3)).show()


    @udtf(returnType="num: int, squared: int")
    class SquareNumbers:
        def eval(self, start: int, end: int):
            for num in range(start, end + 1):
                yield num, num * num


    SquareNumbers(lit(1), lit(3)).show()
