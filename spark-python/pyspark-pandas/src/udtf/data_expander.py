import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf, lit

os.environ["PYSPARK_PYTHON"] = "C:\\Users\\mahfo\\.virtualenvs\\PysparkPandas-bh8Y5n2t\\Scripts\\python.exe"


@udtf(returnType="date: string")
class DateExpander:
    def eval(self, start_date: str, end_date: str):
        current = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        while current <= end:
            yield (current.strftime('%Y-%m-%d'),)
            current += timedelta(days=1)


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    DateExpander(lit("2023-02-25"), lit("2023-03-01")).show()
