import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf

os.environ["PYSPARK_PYTHON"] = "C:\\Users\\mahfo\\.virtualenvs\\PysparkPandas-bh8Y5n2t\\Scripts\\python.exe"


@udtf(returnType="cnt: int")
class CountUDTF:
    def __init__(self):
        # Initialize the counter to 0 when an instance of the class is created.
        self.count = 0

    def eval(self, x: int):
        # Increment the counter by 1 for each input value received.
        self.count += 1

    def terminate(self):
        # Yield the final count when the UDTF is done processing.
        yield self.count,


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    spark.udtf.register("count_udtf", CountUDTF)
    spark.sql("SELECT * FROM range(0, 10, 1, 1), LATERAL count_udtf(id)").show()
    spark.sql("SELECT * FROM range(0, 10, 1, 2), LATERAL count_udtf(id)").show()
