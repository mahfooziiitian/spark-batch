from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName("structured-streaming-result-table")
             .getOrCreate())

    """The `lines` DataFrame represents an unbounded table containing the streaming text data. This table contains 
    one column of strings named “value”, and each line in the streaming text data becomes a row in the table."""
    lines = (spark
             .readStream
             .format("socket")
             .option("host", "localhost")
             .option("port", 9999)
             .load())

    # Split the lines into words
    words = lines.select(
        explode(
            split(lines.value, " ")
        ).alias("word")
    )

    # Generate running word count
    """
    The final wordCounts DataFrame is the result table.
    """
    wordCounts = words.groupBy("word").count()

    """
    We have now set up the query on the streaming data. 
    After this code is executed, the streaming computation will have started in the background.
    Query on streaming lines DataFrame to generate wordCounts is exactly the same as it would be a static DataFrame.
    when this query is started, Spark will continuously check for new data from the socket connection.
    If there is new data, Spark will run an "incremental" query that combines the previous running counts with the new 
    data to compute updated counts.
    """
    query = (wordCounts
             .writeStream
             .outputMode("complete")
             .format("console")
             .start())

    """
    The query object is a handle to that active streaming query, and we have decided to wait for the termination of the
    query using awaitTermination() to prevent the process from exiting while the query is active.
    """
    query.awaitTermination()
