"""
Tests for the ETL pipeline in data_frame.etl.etl.
"""

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_frame.etl.etl import transform


def test_transform_filters_active_only(spark):
    schema = StructType(
        [
            StructField("order_id", IntegerType(), False),
            StructField("customer_id", IntegerType(), True),
            StructField("product", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("status", StringType(), True),
        ]
    )
    data = [
        (1, 1, "Widget", 2, 10.0, "active"),
        (2, 1, "Gadget", 1, 50.0, "active"),
        (3, 2, "Widget", 3, 10.0, "inactive"),
        (4, 2, "Book", 5, 5.0, "active"),
    ]
    df = spark.createDataFrame(data, schema)
    result = transform(df)

    assert result.filter(result["customer_id"] == 1).first()[
        "total_spend"
    ] == pytest.approx(70.0)
    assert result.filter(result["customer_id"] == 2).first()["order_count"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
