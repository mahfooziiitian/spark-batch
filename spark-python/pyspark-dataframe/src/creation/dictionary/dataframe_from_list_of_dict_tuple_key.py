import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.appName("example").getOrCreate()

    # Sample list of dictionaries
    dic = {
        (u'aaa', u'bbb', u'ccc'): ((0.3, 1.2, 1.3, 1.5), 1.4, 1),
        (u'kkk', u'ggg', u'ccc', u'sss'): ((0.6, 1.2, 1.7, 1.5), 1.4, 2)
    }

    # Create DataFrame
    df = spark.sparkContext.parallelize([
        (list(k),) +
        v[0] +
        v[1:]
        for k, v in dic.items()
    ]).toDF(['key', 'val_1', 'val_2', 'val_3', 'val_4', 'val_5', 'val_6'])

    df.printSchema()
    
    # Show DataFrame
    df.show()

    spark.stop()
