import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("functions") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_aggregation(spark):
    studentMarksData = [["si1", "year1", 62.08, 62.4],
                        ["si1", "year2", 75.94, 76.75],
                        ["si2", "year1", 68.26, 72.95],
                        ["si2", "year2", 85.49, 75.8],
                        ["si3", "year1", 75.08, 79.84],
                        ["si3", "year2", 54.98, 87.72],
                        ["si4", "year1", 50.03, 66.85],
                        ["si4", "year2", 71.26, 69.77],
                        ["si5", "year1", 52.74, 76.27],
                        ["si5", "year2", 50.39, 68.58],
                        ["si6", "year1", 74.86, 60.8],
                        ["si6", "year2", 58.29, 62.38],
                        ["si7", "year1", 63.95, 74.51],
                        ["si7", "year2", 66.69, 56.92]]

    studentMarksDataRDD = spark.sparkContext.parallelize(studentMarksData, 4)
    studentMarksMean = studentMarksDataRDD.map(lambda x:
                                               [x[0], x[1], (x[2] + x[3]) / 2])
    expected_data = [['si1', 'year1', 62.239999999999995], ['si1', 'year2', 76.345]]
    assert studentMarksMean.takeOrdered(2) == expected_data


def test_aggregation_filter(spark):
    studentMarksData = [["si1", "year1", 62.08, 62.4],
                        ["si1", "year2", 75.94, 76.75],
                        ["si2", "year1", 68.26, 72.95],
                        ["si2", "year2", 85.49, 75.8],
                        ["si3", "year1", 75.08, 79.84],
                        ["si3", "year2", 54.98, 87.72],
                        ["si4", "year1", 50.03, 66.85],
                        ["si4", "year2", 71.26, 69.77],
                        ["si5", "year1", 52.74, 76.27],
                        ["si5", "year2", 50.39, 68.58],
                        ["si6", "year1", 74.86, 60.8],
                        ["si6", "year2", 58.29, 62.38],
                        ["si7", "year1", 63.95, 74.51],
                        ["si7", "year2", 66.69, 56.92]]

    studentMarksDataRDD = spark.sparkContext.parallelize(studentMarksData, 4)
    studentMarksMean = studentMarksDataRDD.map(lambda x:
                                               [x[0], x[1], (x[2] + x[3]) / 2])
    expected_data = [['si1', 'year2', 76.345], ['si2', 'year2', 80.645]]
    assert studentMarksMean.filter(lambda x: "year2" in x).takeOrdered(2) == expected_data


def test_aggregation_top_k(spark):
    studentMarksData = [["si1", "year1", 62.08, 62.4],
                        ["si1", "year2", 75.94, 76.75],
                        ["si2", "year1", 68.26, 72.95],
                        ["si2", "year2", 85.49, 75.8],
                        ["si3", "year1", 75.08, 79.84],
                        ["si3", "year2", 54.98, 87.72],
                        ["si4", "year1", 50.03, 66.85],
                        ["si4", "year2", 71.26, 69.77],
                        ["si5", "year1", 52.74, 76.27],
                        ["si5", "year2", 50.39, 68.58],
                        ["si6", "year1", 74.86, 60.8],
                        ["si6", "year2", 58.29, 62.38],
                        ["si7", "year1", 63.95, 74.51],
                        ["si7", "year2", 66.69, 56.92]]

    studentMarksDataRDD = spark.sparkContext.parallelize(studentMarksData, 4)
    studentMarksMean = studentMarksDataRDD.map(lambda x:
                                               [x[0], x[1], (x[2] + x[3]) / 2])
    secondYearMarks = studentMarksMean.filter(lambda x: "year2" in x)
    sortedMarksData = secondYearMarks.sortBy(keyfunc=lambda x: -x[2])
    expected_data = [['si2', 'year2', 80.645], ['si1', 'year2', 76.345], ['si3', 'year2', 71.35]]
    assert sortedMarksData.take(3) == expected_data


def test_aggregation_top_k_optimized(spark):
    studentMarksData = [["si1", "year1", 62.08, 62.4],
                        ["si1", "year2", 75.94, 76.75],
                        ["si2", "year1", 68.26, 72.95],
                        ["si2", "year2", 85.49, 75.8],
                        ["si3", "year1", 75.08, 79.84],
                        ["si3", "year2", 54.98, 87.72],
                        ["si4", "year1", 50.03, 66.85],
                        ["si4", "year2", 71.26, 69.77],
                        ["si5", "year1", 52.74, 76.27],
                        ["si5", "year2", 50.39, 68.58],
                        ["si6", "year1", 74.86, 60.8],
                        ["si6", "year2", 58.29, 62.38],
                        ["si7", "year1", 63.95, 74.51],
                        ["si7", "year2", 66.69, 56.92]]

    studentMarksDataRDD = spark.sparkContext.parallelize(studentMarksData, 4)
    studentMarksMean = studentMarksDataRDD.map(lambda x:
                                               [x[0], x[1], (x[2] + x[3]) / 2])
    secondYearMarks = studentMarksMean.filter(lambda x: "year2" in x)
    topThreeStudents = secondYearMarks.takeOrdered(num=3, key=lambda x: -x[2])
    expected_data = [['si2', 'year2', 80.645], ['si1', 'year2', 76.345], ['si3', 'year2', 71.35]]
    assert topThreeStudents == expected_data


def test_aggregation_bottom_k_optimized(spark):
    studentMarksData = [["si1", "year1", 62.08, 62.4],
                        ["si1", "year2", 75.94, 76.75],
                        ["si2", "year1", 68.26, 72.95],
                        ["si2", "year2", 85.49, 75.8],
                        ["si3", "year1", 75.08, 79.84],
                        ["si3", "year2", 54.98, 87.72],
                        ["si4", "year1", 50.03, 66.85],
                        ["si4", "year2", 71.26, 69.77],
                        ["si5", "year1", 52.74, 76.27],
                        ["si5", "year2", 50.39, 68.58],
                        ["si6", "year1", 74.86, 60.8],
                        ["si6", "year2", 58.29, 62.38],
                        ["si7", "year1", 63.95, 74.51],
                        ["si7", "year2", 66.69, 56.92]]

    studentMarksDataRDD = spark.sparkContext.parallelize(studentMarksData, 4)
    studentMarksMean = studentMarksDataRDD.map(lambda x:
                                               [x[0], x[1], (x[2] + x[3]) / 2])
    secondYearMarks = studentMarksMean.filter(lambda x: "year2" in x)
    topThreeStudents = secondYearMarks.takeOrdered(num=3, key=lambda x: x[2])
    expected_data = [['si5', 'year2', 59.485], ['si6', 'year2', 60.335], ['si7', 'year2', 61.805]]
    assert topThreeStudents == expected_data


def test_aggregation_filter_overall(spark):
    studentMarksData = [["si1", "year1", 62.08, 62.4],
                        ["si1", "year2", 75.94, 76.75],
                        ["si2", "year1", 68.26, 72.95],
                        ["si2", "year2", 85.49, 75.8],
                        ["si3", "year1", 75.08, 79.84],
                        ["si3", "year2", 54.98, 87.72],
                        ["si4", "year1", 50.03, 66.85],
                        ["si4", "year2", 71.26, 69.77],
                        ["si5", "year1", 52.74, 76.27],
                        ["si5", "year2", 50.39, 68.58],
                        ["si6", "year1", 74.86, 60.8],
                        ["si6", "year2", 58.29, 62.38],
                        ["si7", "year1", 63.95, 74.51],
                        ["si7", "year2", 66.69, 56.92]]

    studentMarksDataRDD = spark.sparkContext.parallelize(studentMarksData, 4)
    studentMarksMean = studentMarksDataRDD.map(lambda x:
                                               [x[0], x[1], (x[2] + x[3]) / 2])
    secondYearMarks = studentMarksMean.filter(lambda x: "year2" in x)
    moreThan80Marks = secondYearMarks.filter(lambda x: x[2] > 80)
    expected_data = [['si2', 'year2', 80.645]]
    assert moreThan80Marks.collect() == expected_data
