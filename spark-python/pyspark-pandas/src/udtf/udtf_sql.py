import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf

os.environ["PYSPARK_PYTHON"] = "C:\\Users\\mahfo\\.virtualenvs\\PysparkPandas-bh8Y5n2t\\Scripts\\python.exe"

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()


    @udtf(returnType="word: string")
    class WordSplitter:
        def eval(self, text: str):
            for word in text.split(" "):
                yield (word.strip(),)


    # Register the UDTF for use in Spark SQL.
    spark.udtf.register("split_words", WordSplitter)

    # Example: Using the UDTF in SQL.
    spark.sql("SELECT * FROM split_words('hello world')").show()

    spark.sql(
        "SELECT * FROM VALUES ('Hello World'), ('Apache Spark') t(text), "
        "LATERAL split_words(text)"
    ).show()
