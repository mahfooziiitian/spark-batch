import pytest
from pyspark.sql import Row

from data_frame.columns.column_operation import convert_date_columns


def test_convert_date_columns(spark):
    data = [
        Row(
            id=1,
            date_str="2023-01-01",
            timestamp_str="2023-01-01 12:00:00",
            int_str="100",
        )
    ]
    df = spark.createDataFrame(data)

    result_df = convert_date_columns(
        df, ["date_str"], ["timestamp_str"], ["int_str"], "yyyy-MM-dd HH:mm:ss"
    )

    assert str(result_df.schema["date_str"].dataType).startswith("DateType")
    assert str(result_df.schema["timestamp_str"].dataType).startswith("TimestampType")
    assert str(result_df.schema["int_str"].dataType).startswith("IntegerType")


if __name__ == "__main__":
    import pytest

    pytest.main([__file__, "-v", "--tb=short"])
