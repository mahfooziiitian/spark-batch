"""UDTF in Spark SQL — register and use UDTFs from SQL queries.

Demonstrates registering a UDTF and calling it from Spark SQL,
including LATERAL joins.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf

from spp.session import create_spark_session


@udtf(returnType="word: string")
class WordSplitter:
    def eval(self, text: str):
        for word in text.split():
            yield (word.strip(),)


def main(spark: SparkSession) -> None:
    spark.udtf.register("split_words", WordSplitter)

    print("=== Basic SQL call ===")
    spark.sql("SELECT * FROM split_words('hello world from spark')").show()

    print("=== LATERAL join with table data ===")
    spark.sql("""
        SELECT t.text, w.word
        FROM VALUES ('Apache Spark'), ('Pandas on Spark'), ('Arrow UDF') AS t(text),
        LATERAL split_words(t.text) w
    """).show()


if __name__ == "__main__":
    spark = create_spark_session("udtf-sql")
    main(spark)
    spark.stop()
