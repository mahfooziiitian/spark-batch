"""
Apache Spark already performs data processing in parallel, what’s new in it.

If this is the case, please allow me to give an idea about spark job — It is a parallel computation which gets created once a spark action is invoked in an application.

Apart from this, it is a known fact that by default, Apache Spark runs multiple tasks among each executor to achieve parallelism, however, it is not true at job level.

In other words, once a spark action is invoked, a spark job comes into existence which consists of one or more stages and further these stages are broken down into numerous tasks which
are worked upon by the executors in parallel.

Hence, at a time, Spark runs multiple tasks in parallel but not multiple jobs.

WARNING: It does not mean spark cannot run concurrent jobs.

we will explore how we can boost our default spark application’s performance by running multiple jobs(spark actions) at once.

For the same amount of work, spark application with concurrent jobs took just one fourth of the default spark application’s time .

We want to query 3 different tables and save their CSV outputs.( As it involves only one action i.e ‘save’ for each query, it means it would result in 3 jobs in spark UI.)

To implement the aforementioned statement, we have below options.

different Spark sessions for each query — very inefficient and costly idea.
same Spark Session and execute the queries in a loop i.e. a default nature of spark application.
same Spark Session and run the queries in parallel — very efficient as compared to the other two. Lets’s go ahead with it.

By running concurrent jobs with a single spark session, will not only maximise the resource utilisation but also reduce application time and cost drastically.

"""
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor
import os

os.environ["PYSPARK_PYTHON"] = "C:\\Users\\mahfo\\.virtualenvs\\pyspark-parallel-queries-HPhAwGJb\\Scripts\\python.exe"
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.appName("ParallelQueries").getOrCreate()

    # Assume you have a DataFrame named df, you can replace it with your own DataFrame
    csv_data_file = f"{os.environ['DATa_HOME']}/FileData/Csv/OnlineRetail.csv"
    df = spark.read.csv(csv_data_file, header=True, inferSchema=True)

    # Perform some transformations and create multiple DataFrames
    df1 = df.filter(df['UnitPrice'] > 2.25)
    df2 = df.filter(df['InvoiceDate'] >= '12-01-2010 08:26')
    df3 = df.groupBy('Country').agg({'Quantity': 'avg'})

    # Run transformations in parallel using separate threads or processes
    # Note: This example uses separate variables, but you can use a list for more dynamic parallel processing.

    # Use threads

    with ThreadPoolExecutor() as executor:
        result1 = executor.submit(lambda: df1.count())
        result2 = executor.submit(lambda: df2.count())
        result3 = executor.submit(lambda: df3.count())

    # Get results
    count1 = result1.result()
    count2 = result2.result()
    count3 = result3.result()

    # Print results
    print(f"Count for df1: {count1}")
    print(f"Count for df2: {count2}")
    print(f"Count for df3: {count3}")

    # Stop the Spark session
    spark.stop()
