"""PySpark Pandas — entry-point overview example.

Demonstrates a basic pandas UDF (grouped aggregate) using Arrow-optimized
Spark ↔ pandas transfer.
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf

from spp.session import create_spark_session


def main(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ["id", "value"],
    )

    @pandas_udf("double")
    def mean_udf(v: pd.Series) -> float:
        return v.mean()

    # pandas aggregate UDFs cannot be mixed with built-in aggregate functions
    avg_result = df.groupBy("id").agg(mean_udf(df["value"]).alias("avg_value"))
    count_result = df.groupBy("id").agg(F.count("value").alias("count"))

    result = avg_result.join(count_result, on="id")
    result.show()


if __name__ == "__main__":
    spark = create_spark_session("pyspark-pandas-overview")
    main(spark)
    spark.stop()
