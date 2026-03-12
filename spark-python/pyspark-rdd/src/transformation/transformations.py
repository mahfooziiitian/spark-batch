from pyspark.sql import SparkSession


def create_spark_session():
    return SparkSession.builder.getOrCreate()


def get_student_data():
    return [
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


def create_student_rdd(spark, data):
    return spark.sparkContext.parallelize(data)


def transform_grades(rdd):
    return rdd.map(lambda x: (x[0], x[1], int(x[2] * 100), x[3]))


def filter_high_grades(rdd, threshold=80):
    return rdd.filter(lambda x: x[2] > threshold)


def print_rdd(rdd, title):
    print(f"\n{title}:")
    for row in rdd.collect():
        print(row)


def main():
    spark = create_spark_session()
    data = get_student_data()
    student_rdd = create_student_rdd(spark, data)
    rdd_transformation = transform_grades(student_rdd)
    print_rdd(rdd_transformation, "Transformed Grades")
    rdd_filtered = filter_high_grades(rdd_transformation)
    print_rdd(rdd_filtered, "Filtered Grades > 80")
    spark.stop()


if __name__ == "__main__":
    main()
