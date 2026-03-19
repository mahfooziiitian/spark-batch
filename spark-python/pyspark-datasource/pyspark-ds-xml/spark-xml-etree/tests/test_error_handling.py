"""Tests for robust XML parsing with error handling."""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

from spark_etree.xmls_error_handling import (
    PRODUCT_SCHEMA,
    SAMPLE_DATA,
    safe_parse_product,
)


class TestSafeParseProduct:
    """Tests for safe_parse_product pure function."""

    def test_valid_xml_all_fields(self):
        xml = '<product sku="P1"><name>Widget</name><price>19.99</price><category>Tools</category></product>'
        result = safe_parse_product(xml)
        assert result["sku"] == "P1"
        assert result["name"] == "Widget"
        assert result["price"] == "19.99"
        assert result["category"] == "Tools"
        assert result["parse_error"] is None

    def test_missing_optional_field(self):
        xml = '<product sku="P2"><name>Gadget</name></product>'
        result = safe_parse_product(xml)
        assert result["name"] == "Gadget"
        assert result["price"] is None
        assert result["category"] is None
        assert result["parse_error"] is None

    def test_missing_sku_attribute(self):
        xml = "<product><name>NoSKU</name></product>"
        result = safe_parse_product(xml)
        assert result["sku"] is None
        assert result["name"] == "NoSKU"
        assert result["parse_error"] is None

    def test_malformed_xml_returns_error(self):
        result = safe_parse_product("not xml")
        assert result["parse_error"] is not None
        assert result["sku"] is None
        assert result["name"] is None

    def test_none_input_returns_error(self):
        result = safe_parse_product(None)
        assert result["parse_error"] == "empty or null input"

    def test_empty_string_returns_error(self):
        result = safe_parse_product("")
        assert result["parse_error"] == "empty or null input"

    def test_empty_name_element(self):
        xml = '<product sku="P3"><name></name><price>5.00</price></product>'
        result = safe_parse_product(xml)
        assert result["name"] is None
        assert result["price"] == "5.00"
        assert result["parse_error"] is None


class TestSafeParseProductSpark:
    """Integration tests running safe_parse_product through Spark UDFs."""

    def test_total_row_count_preserved(self, spark):
        df = spark.createDataFrame(SAMPLE_DATA)
        safe_parse_udf = udf(safe_parse_product, PRODUCT_SCHEMA)

        parsed = df.withColumn("product", safe_parse_udf("xml")).select("id", "product.*")
        assert parsed.count() == len(SAMPLE_DATA)

    def test_clean_error_split(self, spark):
        df = spark.createDataFrame(SAMPLE_DATA)
        safe_parse_udf = udf(safe_parse_product, PRODUCT_SCHEMA)

        parsed = df.withColumn("product", safe_parse_udf("xml")).select("id", "product.*")
        clean = parsed.filter(F.col("parse_error").isNull())
        errors = parsed.filter(F.col("parse_error").isNotNull())

        # ids 1,2,4,7,8 parse OK (5 clean), ids 3,5,6 error (3 errors)
        assert clean.count() == 5
        assert errors.count() == 3

    def test_error_ids(self, spark):
        df = spark.createDataFrame(SAMPLE_DATA)
        safe_parse_udf = udf(safe_parse_product, PRODUCT_SCHEMA)

        error_ids = (
            df.withColumn("product", safe_parse_udf("xml"))
            .select("id", "product.parse_error")
            .filter(F.col("parse_error").isNotNull())
            .select("id")
            .orderBy("id")
            .collect()
        )
        assert [r["id"] for r in error_ids] == [3, 5, 6]

    def test_valid_row_field_values(self, spark):
        df = spark.createDataFrame(SAMPLE_DATA)
        safe_parse_udf = udf(safe_parse_product, PRODUCT_SCHEMA)

        row = df.withColumn("product", safe_parse_udf("xml")).select("id", "product.*").filter(F.col("id") == 1).first()
        assert row["sku"] == "P100"
        assert row["name"] == "Widget"
        assert row["price"] == "19.99"
        assert row["category"] == "Tools"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
