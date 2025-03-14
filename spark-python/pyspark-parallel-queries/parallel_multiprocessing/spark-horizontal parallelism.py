"""
Thus this depends on 2 factors

    No of threads available on driver machine
    No of executors and cores available

Use case Leveraging Horizontal parallelism
We can use this in the following use case

    Purely independent functions dealing on column level
    Writing data to diff partitions

"""
import multiprocessing as ms
import os
import sys
import time
from multiprocessing.pool import ThreadPool

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    csv_data_file = f"{os.environ['DATA_HOME']}/FileData/Csv/titanic.csv"
    df = spark.read.csv(csv_data_file, header=True, inferSchema=True)
    df.show(3)
    opt = []
    tot = df.count()
    print(tot)


    def dups_count(col: str, dataframe=df):
        # print(col, dt.datetime.now())
        opt.append((col, tot - dataframe.drop_duplicates([col]).count()))


    start_time = time.perf_counter()
    for col in df.columns:
        dups_count(col, df)
    end_time = time.perf_counter()
    print(end_time - start_time, "seconds")

    start_time = time.perf_counter()
    pool = ThreadPool(2)
    opt2 = pool.map(dups_count, df.columns)
    end_time = time.perf_counter()
    print(end_time - start_time, "seconds")

    start_time = time.perf_counter()
    pool = ThreadPool(ms.cpu_count())
    pool.map(dups_count, df.columns)
    end_time = time.perf_counter()
    print(end_time - start_time, "seconds")

    start_time = time.perf_counter()
    pool = ThreadPool(len(df.columns))
    pool.map(dups_count, df.columns)
    end_time = time.perf_counter()
    print(end_time - start_time, "seconds")
