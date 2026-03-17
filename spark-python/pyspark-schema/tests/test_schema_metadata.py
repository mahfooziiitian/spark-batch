import json

import pytest
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from schema_metadata import get_pii_columns, mask_pii

SCHEMA = StructType([
    StructField("id", LongType(), nullable=False,
                metadata={"description": "Order ID", "pii": False}),
    StructField("customer_email", StringType(), nullable=True,
                metadata={"description": "Customer email", "pii": True,
                          "classification": "confidential"}),
    StructField("amount", DoubleType(), nullable=True,
                metadata={"description": "Order total", "pii": False, "unit": "USD"}),
    StructField("is_paid", BooleanType(), nullable=True,
                metadata={"description": "Payment flag", "pii": False}),
])

DATA = [
    (1, "alice@example.com", 99.99,  True),
    (2, "bob@example.com",   149.00, False),
]


class TestStructFieldMetadata:
    def test_metadata_accessible_on_field(self):
        assert SCHEMA["id"].metadata["description"]    == "Order ID"
        assert SCHEMA["id"].metadata["pii"]            is False
        assert SCHEMA["customer_email"].metadata["pii"] is True

    def test_metadata_classification(self):
        assert SCHEMA["customer_email"].metadata["classification"] == "confidential"

    def test_metadata_unit(self):
        assert SCHEMA["amount"].metadata["unit"] == "USD"

    def test_missing_key_returns_none(self):
        assert SCHEMA["id"].metadata.get("nonexistent") is None

    def test_metadata_preserved_in_dataframe(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        assert df.schema["id"].metadata["pii"] is False
        assert df.schema["customer_email"].metadata["pii"] is True

    def test_metadata_survives_json_roundtrip(self):
        schema_back = StructType.fromJson(json.loads(SCHEMA.json()))
        assert schema_back["customer_email"].metadata["pii"]            is True
        assert schema_back["customer_email"].metadata["classification"] == "confidential"
        assert schema_back["amount"].metadata["unit"]                   == "USD"


class TestGetPIIColumns:
    def test_returns_only_pii_columns(self):
        pii = get_pii_columns(SCHEMA)
        assert "customer_email" in pii
        assert "id"      not in pii
        assert "amount"  not in pii
        assert "is_paid" not in pii

    def test_returns_list(self):
        assert isinstance(get_pii_columns(SCHEMA), list)

    def test_no_pii_columns_returns_empty(self):
        no_pii = StructType([
            StructField("a", StringType(), nullable=True, metadata={"pii": False}),
            StructField("b", StringType(), nullable=True, metadata={}),
        ])
        assert get_pii_columns(no_pii) == []

    def test_all_pii_columns_returned(self):
        all_pii = StructType([
            StructField("x", StringType(), nullable=True, metadata={"pii": True}),
            StructField("y", StringType(), nullable=True, metadata={"pii": True}),
        ])
        assert sorted(get_pii_columns(all_pii)) == ["x", "y"]


class TestMaskPII:
    def test_pii_column_redacted(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        masked = mask_pii(df, SCHEMA)
        emails = {r["customer_email"] for r in masked.select("customer_email").collect()}
        assert emails == {"***REDACTED***"}

    def test_non_pii_column_unchanged(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        masked = mask_pii(df, SCHEMA)
        ids = {r["id"] for r in masked.select("id").collect()}
        assert ids == {1, 2}

    def test_row_count_preserved(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        assert mask_pii(df, SCHEMA).count() == df.count()

    def test_all_pii_values_replaced(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        masked = mask_pii(df, SCHEMA)
        non_redacted = (masked
                        .filter(masked["customer_email"] != "***REDACTED***")
                        .count())
        assert non_redacted == 0

    def test_returns_dataframe(self, spark):
        from pyspark.sql import DataFrame
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        result = mask_pii(df, SCHEMA)
        assert isinstance(result, DataFrame)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
