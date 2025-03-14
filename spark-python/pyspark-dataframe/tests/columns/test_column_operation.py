import pytest
from pyspark.sql import Row, SparkSession

from columns.column_operation import convert_date_columns


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("df_column")
        .getOrCreate()
    )


# def test_add_audit_col(spark):
#     # Sample data
#     data = [Row(id=1, name="Test")]
#     df = spark.createDataFrame(data)

#     # Call the function to test
#     result_df = add_audit_col(df)

#     # Verify the structure and data
#     expected_columns = [
#         "id",
#         "name",
#         "crt_user_id",
#         "crt_dttm",
#         "upd_user_id",
#         "upd_dttm",
#     ]
#     assert set(result_df.columns) == set(
#         expected_columns
#     ), "Audit columns missing or incorrect"


def test_convert_date_columns(spark):
    # Sample data with string representations of dates
    data = [
        Row(
            id=1,
            date_str="2023-01-01",
            timestamp_str="2023-01-01 12:00:00",
            int_str="100",
        )
    ]
    df = spark.createDataFrame(data)

    # Convert columns
    result_df = convert_date_columns(
        df, ["date_str"], ["timestamp_str"], ["int_str"], "yyyy-MM-dd HH:mm:ss"
    )
    print(
        str(result_df.schema["timestamp_str"].dataType),
        str(result_df.schema["int_str"].dataType),
    )

    # Verify the data types
    assert str(result_df.schema["date_str"].dataType).startswith(
        "DateType"
    ), "date_str not converted to DateType"
    assert str(result_df.schema["timestamp_str"].dataType).startswith(
        "TimestampType"
    ), "timestamp_str not converted to TimestampType"
    assert str(result_df.schema["int_str"].dataType).startswith(
        "IntegerType"
    ), "int_str not converted to IntegerType"
