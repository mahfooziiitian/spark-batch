import os

from pydeequ.repository import FileSystemMetricsRepository
import pydeequ
from pydeequ.analyzers import *
from pyspark.sql import Row

if __name__ == '__main__':
    os.environ["SPARK_VERSION"] = "3.0.2"
    print(os.environ["SPARK_VERSION"])
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("functions")
             .config("spark.jars.packages", pydeequ.deequ_maven_coord)
             .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
             .getOrCreate())

    df = spark.sparkContext.parallelize([
        Row(a="foo", b=1, c=5),
        Row(a="bar", b=2, c=6),
        Row(a="baz", b=3, c=None)]).toDF()

    metrics_file = FileSystemMetricsRepository.helper_metrics_file(spark, 'metrics.json')
    repository = FileSystemMetricsRepository(spark, metrics_file)
    key_tags = {'tag': 'pydeequ hello world'}
    resultKey = ResultKey(spark, ResultKey.current_milli_time(), key_tags)

    analysisResult = AnalysisRunner(spark) \
        .onData(df) \
        .addAnalyzer(ApproxCountDistinct('b')) \
        .useRepository(repository) \
        .saveOrAppendResult(resultKey) \
        .run()

    result_metre_df = repository.load() \
        .before(ResultKey.current_milli_time()) \
        .forAnalyzers([ApproxCountDistinct('b')]) \
        .getSuccessMetricsAsDataFrame()

    spark.sparkContext._gateway.shutdown_callback_server()
    spark.stop()
