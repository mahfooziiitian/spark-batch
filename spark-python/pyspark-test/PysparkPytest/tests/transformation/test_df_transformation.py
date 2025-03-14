from pyspark.testing import assertDataFrameEqual

from transformation.df_transformation import remove_extra_spaces


def test_single_space(spark):
    sample_data = [{"name": "John    D.", "age": 30},
                   {"name": "Alice   G.", "age": 25},
                   {"name": "Bob  T.", "age": 35},
                   {"name": "Eve   A.", "age": 28}]

    # Create a Spark DataFrame
    original_df = spark.createDataFrame(sample_data)

    # Apply the transformation function from before
    transformed_df = remove_extra_spaces(original_df, "name")

    expected_data = [{"name": "John D.", "age": 30},
                     {"name": "Alice G.", "age": 25},
                     {"name": "Bob T.", "age": 35},
                     {"name": "Eve A.", "age": 28}]

    expected_df = spark.createDataFrame(expected_data)

    assert assertDataFrameEqual(transformed_df, expected_df)


