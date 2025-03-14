from pyspark.sql import Row, SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException


def has_column(df: DataFrame, col):
    is_contain_column = False
    try:
        df_column = df[col]
        is_contain_column = True
    except AnalysisException:
        is_contain_column = False
    return is_contain_column


if __name__ == '__main__':
    # create an app named array schema fields
    spark = SparkSession.builder.appName('array-schema-fields').getOrCreate()
    df = spark.sparkContext.parallelize([Row(foo=[Row(bar=Row(foobar=3))])]).toDF()

    print(has_column(df, "foobar"))

    print(has_column(df, "foo"))

    print(has_column(df, "foo.bar"))

    print(has_column(df, "foo.bar.foobar"))

    print(has_column(df, "foo.bar.foobaz"))

