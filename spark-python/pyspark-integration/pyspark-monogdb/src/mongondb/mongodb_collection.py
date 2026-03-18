import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:mongo@127.0.0.1:27017")
MONGO_DB = os.environ.get("MONGO_DB", "tutorial")
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("mongodb-collection")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",
        )
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    people = spark.createDataFrame(
        [
            ("Bilbo Baggins", 50),
            ("Gandalf", 1000),
            ("Thorin", 195),
            ("Balin", 178),
            ("Kili", 77),
            ("Dwalin", 169),
            ("Oin", 167),
            ("Gloin", 158),
            ("Fili", 82),
            ("Bombur", None),
        ],
        ["name", "age"],
    )

    people.printSchema()
    people.show()

    # Write to MongoDB
    (people.write
     .format("mongodb")
     .mode("overwrite")
     .option("database", MONGO_DB)
     .option("collection", "people")
     .save())

    # Read back from MongoDB
    people_from_mongo = (
        spark.read
        .format("mongodb")
        .option("database", MONGO_DB)
        .option("collection", "people")
        .load()
    )

    print("People read from MongoDB:")
    people_from_mongo.show()

    # Filter: characters with known age above 100
    elders = (
        people_from_mongo
        .filter(F.col("age").isNotNull())
        .filter(F.col("age") > 100)
        .orderBy(F.desc("age"))
    )

    print("Characters older than 100:")
    elders.show()

    # Write filtered results to a separate collection
    (elders.write
     .format("mongodb")
     .mode("overwrite")
     .option("database", MONGO_DB)
     .option("collection", "elders")
     .save())

    spark.stop()


if __name__ == "__main__":
    main()
