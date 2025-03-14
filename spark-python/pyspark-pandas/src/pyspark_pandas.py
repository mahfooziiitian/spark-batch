import os

import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "C:\\Users\\mahfo\\.virtualenvs\\PysparkPandas-bh8Y5n2t\\Scripts\\python.exe"


def main(spark):
    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ("id", "v"))

    @pandas_udf("double")
    def mean_udf(v: pd.Series) -> float:
        return v.mean()

    print(df.groupby("id").agg(mean_udf(df['v'])).collect())


if __name__ == "__main__":
    main(SparkSession.builder.getOrCreate())
