"""Tests for XML attribute extraction and array explode patterns."""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, IntegerType

from spark_etree.xmls_data_processing_multiple_column2 import (
    SAMPLE_DATA,
    extract_attributes,
    extract_record_ids,
)


class TestExtractAttributes:
    """Tests for extract_attributes decorator-based UDF."""

    def test_extracts_a_and_b_attributes(self, spark):
        data = [{"id": 1, "data": '<test a="10" b="20"><records /></test>'}]
        df = spark.createDataFrame(data)
        result = df.withColumn("attrs", extract_attributes(F.col("data"))).collect()
        assert result[0]["attrs"] == ["10", "20"]

    def test_all_sample_rows_produce_attributes(self, spark):
        df = spark.createDataFrame(SAMPLE_DATA)
        result = df.withColumn("attrs", extract_attributes(F.col("data"))).orderBy("id").collect()
        assert result[0]["attrs"] == ["100", "200"]
        assert result[1]["attrs"] == ["200", "400"]
        assert result[2]["attrs"] == ["300", "600"]


class TestExtractRecordIds:
    """Tests for extract_record_ids pure function."""

    def test_extracts_record_ids(self):
        xml = '<test a="1" b="2"><records><record id="10" /><record id="20" /></records></test>'
        assert extract_record_ids(xml) == [10, 20]

    def test_empty_records(self):
        xml = '<test a="1" b="2"><records /></test>'
        assert extract_record_ids(xml) == []

    def test_single_record(self):
        xml = '<test a="1" b="2"><records><record id="99" /></records></test>'
        assert extract_record_ids(xml) == [99]


class TestExplodeRecordIds:
    """Tests for exploding record IDs through Spark UDF."""

    def test_explode_produces_correct_row_count(self, spark):
        df = spark.createDataFrame(SAMPLE_DATA)
        extract_udf = udf(extract_record_ids, ArrayType(IntegerType()))

        exploded = (
            df.withColumn("record_ids", extract_udf(F.col("data")))
            .withColumn("record_id", F.explode("record_ids"))
            .select("id", "record_id")
        )
        # id=1 has 2 records, id=2 has 2, id=3 has 3 → total 7
        assert exploded.count() == 7

    def test_explode_preserves_parent_id(self, spark):
        df = spark.createDataFrame(SAMPLE_DATA)
        extract_udf = udf(extract_record_ids, ArrayType(IntegerType()))

        result = (
            df.withColumn("record_ids", extract_udf(F.col("data")))
            .withColumn("record_id", F.explode("record_ids"))
            .select("id", "record_id")
            .filter(F.col("id") == 3)
            .orderBy("record_id")
            .collect()
        )
        assert [r["record_id"] for r in result] == [303, 603, 903]

    def test_record_ids_for_first_row(self, spark):
        df = spark.createDataFrame(SAMPLE_DATA[:1])
        extract_udf = udf(extract_record_ids, ArrayType(IntegerType()))

        result = df.withColumn("record_ids", extract_udf(F.col("data"))).select("record_ids").collect()
        assert result[0]["record_ids"] == [101, 201]


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
