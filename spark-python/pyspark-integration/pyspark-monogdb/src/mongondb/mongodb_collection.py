from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("mongodb_collection") \
       .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.13:10.1.1')\
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/tutorial.people") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/tutorial.people_out") \
        .getOrCreate()

    # creating dataframe
    people = spark.createDataFrame([("Bilbo Baggins",  50), ("Gandalf", 1000),
                                    ("Thorin", 195), ("Balin", 178), ("Kili", 77),
                                    ("Dwalin", 169), ("Oin", 167),
                                    ("Gloin", 158), ("Fili", 82),
                                    ("Bombur", None)], ["name", "age"])

    # Writing to mongodb
    people.write.format("mongodb").\
        mode("append").\
        option("database", "tutorial").\
        option("collection", "people_out").\
        save()


# end main
