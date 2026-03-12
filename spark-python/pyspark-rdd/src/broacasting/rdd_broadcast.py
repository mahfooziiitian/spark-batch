from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.getOrCreate()
    student_data = [
        ("Chris", 1523, 0.72, "CA"),
        ("Jake", 1555, 0.83, "NY"),
        ("Cody", 1439, 0.92, "CA"),
        ("Lisa", 1442, 0.81, "FL"),
        ("Daniel", 1600, 0.88, "TX"),
        ("Kelvin", 1382, 0.99, "FL"),
        ("Nancy", 1442, 0.74, "TX"),
        ("Pavel", 1599, 0.82, "NY"),
        ("Josh", 1482, 0.78, "CA"),
        ("Cynthia", 1582, 0.94, "CA"),
    ]
    student_rdd = spark.sparkContext.parallelize(student_data)
    rdd_transformation = student_rdd.map(lambda x: (x[0], x[1], int(x[2] * 100), x[3]))

    states = {"NY": "New York", "CA": "California", "TX": "Texas", "FL": "Florida"}

    broadcastStates = spark.sparkContext.broadcast(states)

    rdd_broadcast = rdd_transformation.map(
        lambda x: (x[0], x[1], x[2], broadcastStates.value.get(x[3], x[3]))
    )

    # confirm transformation is correct
    rdd_broadcast.collect()
    for record in rdd_broadcast.collect():
        print(record)


if __name__ == "__main__":
    main()
