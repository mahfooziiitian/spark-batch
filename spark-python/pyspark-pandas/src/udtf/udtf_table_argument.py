import os

from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf

os.environ["PYSPARK_PYTHON"] = "C:\\Users\\mahfo\\.virtualenvs\\PysparkPandas-bh8Y5n2t\\Scripts\\python.exe"


@udtf(returnType="id: int")
class FilterUDTF:
    def eval(self, row: Row):
        if row["id"] > 5:
            yield row["id"],


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    spark.udtf.register("filter_udtf", FilterUDTF)
    spark.sql("SELECT * FROM filter_udtf(TABLE(SELECT * FROM range(10)))").show()
