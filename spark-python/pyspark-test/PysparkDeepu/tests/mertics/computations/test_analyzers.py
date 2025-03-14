import pydeequ
import pytest
from pydeequ import AnalysisRunner
from pydeequ.analyzers import Size, Completeness, AnalyzerContext
from pyspark.sql import Row
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("functions")
             .config("spark.jars.packages", pydeequ.deequ_maven_coord)
             .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
             .getOrCreate())
    yield spark
    spark.stop()


def test_analyzer(spark):
    df = spark.sparkContext.parallelize([
        Row(a="foo", b=1, c=5),
        Row(a="bar", b=2, c=6),
        Row(a="baz", b=3, c=None)]).toDF()

    analysisResult = AnalysisRunner(spark) \
        .onData(df) \
        .addAnalyzer(Size()) \
        .addAnalyzer(Completeness("b")) \
        .run()

    analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
    analysisResult_df.show()
