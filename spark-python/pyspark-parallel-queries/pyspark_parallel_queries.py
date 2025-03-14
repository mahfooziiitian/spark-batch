import threading

from pyspark.sql import SparkSession


def wordCount(file):
    wordCounts = file.map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1 + v2).toDF(["Word", "Count"])
    wordCounts.write.mode("overwrite").parquet("/outFiles/wordcount")


def charCount(file):
    wordCounts = file.map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1 + v2)
    charCounts = wordCounts.flatMap(lambda pair: pair[0]).map(lambda c: c).map(lambda c: (c, 1)).reduceByKey(
        lambda v1, v2: v1 + v2).toDF()
    charCounts.write.mode("overwrite").parquet("/outFiles/charcount")


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    # Loading input file and breaking into words separated by space.
    file = sc.textFile("/inFiles/shakespeare.txt").flatMap(lambda line: line.split(" ")).cache()

    # Instance variables threads
    # Instantiating thread variables with functions in memory.
    T1 = threading.Thread(target=wordCount, args=(file,))
    T2 = threading.Thread(target=charCount, args=(file,))

    # Starting execution of threads.
    T1.start()
    T2.start()

    # Pausing thread execution to follow main stream. Without them, the thread will run in parallel.
    T1.join()
    T2.join()
