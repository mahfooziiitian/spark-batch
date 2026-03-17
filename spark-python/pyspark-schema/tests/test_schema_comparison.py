import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from schema_comparison import is_backward_compatible, schema_diff

V1 = StructType([
    StructField("id",     LongType(),   nullable=False),
    StructField("name",   StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
])

# v2 adds a nullable column
V2 = StructType([
    StructField("id",     LongType(),   nullable=False),
    StructField("name",   StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("region", StringType(), nullable=True),
])

# broken: id type changed, amount removed
BROKEN = StructType([
    StructField("id",   IntegerType(), nullable=False),
    StructField("name", StringType(),  nullable=True),
])


class TestSchemaDiff:
    def test_identical_schemas_no_diff(self):
        diff = schema_diff(V1, V1)
        assert diff["missing_in_b"]    == []
        assert diff["extra_in_b"]      == []
        assert diff["type_mismatches"] == []

    def test_extra_column_detected(self):
        diff = schema_diff(V1, V2)
        assert "region" in diff["extra_in_b"]
        assert diff["missing_in_b"] == []

    def test_missing_column_detected(self):
        diff = schema_diff(V1, BROKEN)
        assert "amount" in diff["missing_in_b"]

    def test_type_mismatch_detected(self):
        diff = schema_diff(V1, BROKEN)
        mismatches = [m["column"] for m in diff["type_mismatches"]]
        assert "id" in mismatches

    def test_type_mismatch_contains_types(self):
        diff = schema_diff(V1, BROKEN)
        mismatch = next(m for m in diff["type_mismatches"] if m["column"] == "id")
        assert mismatch["type_a"] != mismatch["type_b"]

    def test_nullable_change_detected(self):
        v1_strict = StructType([StructField("id", LongType(), nullable=False)])
        v2_lenient = StructType([StructField("id", LongType(), nullable=True)])
        diff = schema_diff(v1_strict, v2_lenient)
        nullable_cols = [c["column"] for c in diff["nullable_changes"]]
        assert "id" in nullable_cols

    def test_returns_dict_with_required_keys(self):
        diff = schema_diff(V1, V2)
        assert set(diff.keys()) == {
            "missing_in_b", "extra_in_b", "type_mismatches", "nullable_changes"
        }


class TestBackwardCompatibility:
    def test_identical_schemas_compatible(self):
        assert is_backward_compatible(V1, V1) is True

    def test_reader_v1_writer_v2_compatible(self):
        # v1 reader, v2 data — extra 'region' column silently ignored
        assert is_backward_compatible(V1, V2) is True

    def test_reader_v2_writer_v1_compatible(self):
        # v2 reader, v1 data — 'region' missing but it's nullable → OK
        assert is_backward_compatible(V2, V1) is True

    def test_type_mismatch_incompatible(self):
        assert is_backward_compatible(V1, BROKEN) is False

    def test_missing_non_nullable_incompatible(self):
        reader = StructType([
            StructField("id",       LongType(),   nullable=False),
            StructField("required", StringType(), nullable=False),  # NOT NULL
        ])
        writer = StructType([
            StructField("id", LongType(), nullable=False),
            # 'required' absent from writer
        ])
        assert is_backward_compatible(reader, writer) is False

    def test_missing_nullable_column_compatible(self):
        reader = StructType([
            StructField("id",      LongType(),   nullable=False),
            StructField("optional", StringType(), nullable=True),  # nullable → OK
        ])
        writer = StructType([
            StructField("id", LongType(), nullable=False),
        ])
        assert is_backward_compatible(reader, writer) is True

    def test_returns_bool(self):
        result = is_backward_compatible(V1, V2)
        assert isinstance(result, bool)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
