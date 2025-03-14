import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf

os.environ["PYSPARK_PYTHON"] = "C:\\Users\\mahfo\\.virtualenvs\\PysparkPandas-bh8Y5n2t\\Scripts\\python.exe"

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    @udtf(returnType="c1: int, c2: int", useArrow=True)
    class PlusOne:
        def eval(self, x: int):
            yield x, x + 1


    spark.udtf.register("plus_one", PlusOne)

    spark.sql("SELECT * FROM plus_one(1)").show()
