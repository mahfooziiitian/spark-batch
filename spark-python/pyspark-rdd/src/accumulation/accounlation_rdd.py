from pyspark import AccumulatorParam
from pyspark.sql import SparkSession


# Custom accumulator for tracking student names
class StudentListAccumulator(AccumulatorParam):
    def zero(self, value):
        return []

    def addInPlace(self, list1, list2):
        return list1 + list2


# Custom accumulator for tracking statistics
class StatsAccumulator(AccumulatorParam):
    def zero(self, value):
        return {"count": 0, "sum": 0.0, "min": float("inf"), "max": float("-inf")}

    def addInPlace(self, stats1, stats2):
        return {
            "count": stats1["count"] + stats2["count"],
            "sum": stats1["sum"] + stats2["sum"],
            "min": (
                min(stats1["min"], stats2["min"])
                if stats1["min"] != float("inf") and stats2["min"] != float("inf")
                else min(stats1["min"], stats2["min"])
            ),
            "max": (
                max(stats1["max"], stats2["max"])
                if stats1["max"] != float("-inf") and stats2["max"] != float("-inf")
                else max(stats1["max"], stats2["max"])
            ),
        }


def main():
    # Initialize Spark session with app name
    spark = (
        SparkSession.builder.appName("StudentDataAnalysis")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # Student data: (name, SAT_score, GPA, state)
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
    ]

    # Create RDD
    students_rdd = sc.parallelize(student_data)

    # Initialize accumulators
    # Initialize accumulators
    high_performers = sc.accumulator([], StudentListAccumulator())
    sat_stats = sc.accumulator(
        {"count": 0, "sum": 0.0, "min": float("inf"), "max": float("-inf")},
        StatsAccumulator(),
    )

    # Process each student record
    def process_student(record):
        name, sat_score, gpa, state = record

        # Track high performers (SAT > 1500 and GPA > 0.8)
        if sat_score > 1500 and gpa > 0.8:
            high_performers.add([name])

        # Update SAT statistics
        sat_stats.add(
            {
                "count": 1,
                "sum": float(sat_score),
                "min": float(sat_score),
                "max": float(sat_score),
            }
        )

    # Apply the processing function
    students_rdd.foreach(process_student)

    # Display results
    print("High Performers (SAT > 1500 and GPA > 0.8):")
    for student in high_performers.value:
        print(f"  {student}")

    print("\nSAT Score Statistics:")
    stats = sat_stats.value
    print(f"  Count: {stats['count']}")
    print(f"  Average: {stats['sum'] / stats['count']:.2f}")
    print(f"  Min: {stats['min']}")
    print(f"  Max: {stats['max']}")

    spark.stop()


if __name__ == "__main__":
    main()
