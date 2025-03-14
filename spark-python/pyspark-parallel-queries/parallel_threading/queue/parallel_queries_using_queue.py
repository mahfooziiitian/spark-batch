import os
import sys
from threading import Thread
from queue import Queue

from pyspark.sql import SparkSession
os.environ["PYSPARK_DRIVER"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

print(sys.executable)
if __name__ == '__main__':
    spark = (SparkSession.builder.
             config("spark.jars.packages", "mysql:mysql-connector-java:8.0.32").
             getOrCreate())
    q = Queue()
    worker_count = 2
    table_list = ["Badges", "Comments", "LinkTypes", "PostLinks", "Posts", "PostTypes", "Users", "Votes", "VoteTypes"]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS raw_stackoverflow")


    def load_table(table):
        print(table)
        destination_table = "raw_stackoverflow." + table

        df = (
            spark.read
            .format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/tutorials")
            .option("dbtable", table)
            .option("user", "root")
            .option("password", "MySQL_2023")
            .load()
        )

        df.write.format("parquet").mode("overwrite").saveAsTable(destination_table)


    def run_tasks(function, q):
        while not q.empty():
            value = q.get()
            function(value)
            q.task_done()


    print(table_list)

    for table in table_list:
        q.put(table)

    for i in range(worker_count):
        t = Thread(target=run_tasks, args=(load_table, q))
        t.daemon = True
        t.start()

    q.join()
