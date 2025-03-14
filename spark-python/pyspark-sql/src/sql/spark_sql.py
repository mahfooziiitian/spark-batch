from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Data Frame Example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    Student = Row("firstName", "lastName", "age", "telephone")
    s1 = Student('David', 'Julian', 22, 100000)
    s2 = Student('Mark', 'Webb', 23, 658545)
    StudentData = [s1, s2]
    df = spark.createDataFrame(StudentData)
    df.show()
