import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("audit_data") \
        .master("local[*]") \
        .getOrCreate()

    from PySparkAudit import auditing

    data_path = os.environ["DATA_HOME"] + "\\FileData\\Csv\\departuredelays.csv"
    # load dataset
    data = spark.read.csv(path=data_path, sep=',', encoding='UTF-8', comment=None, header=True, inferSchema=True)

    # auditing in one function
    print(auditing(data, display=True))
