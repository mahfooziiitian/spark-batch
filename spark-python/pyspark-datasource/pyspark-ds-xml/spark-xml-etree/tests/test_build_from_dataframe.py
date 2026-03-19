"""Tests for building XML from DataFrame rows and round-tripping."""

import xml.etree.ElementTree as ET

import pytest
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from spark_etree.xmls_build_from_dataframe import (
    SAMPLE_DATA,
    row_to_xml,
    wrap_in_root,
)


class TestRowToXml:
    """Tests for row_to_xml pure function."""

    def test_produces_valid_xml(self):
        result = row_to_xml(101, "Alice", "Engineering", 95000)
        doc = ET.fromstring(result)
        assert doc.tag == "employee"
        assert doc.attrib["id"] == "101"

    def test_contains_correct_elements(self):
        result = row_to_xml(102, "Bob", "Marketing", 72000)
        doc = ET.fromstring(result)
        assert doc.findtext("name") == "Bob"
        assert doc.findtext("department") == "Marketing"
        assert doc.findtext("salary") == "72000"

    def test_none_name_returns_none(self):
        assert row_to_xml(999, None, "Dept", 50000) is None

    def test_zero_salary(self):
        result = row_to_xml(200, "Test", "Dept", 0)
        doc = ET.fromstring(result)
        assert doc.findtext("salary") == "0"


class TestWrapInRoot:
    """Tests for wrap_in_root function."""

    def test_wraps_fragments_under_root(self):
        fragments = [
            '<employee id="1"><name>A</name><department>D</department><salary>1</salary></employee>',
            '<employee id="2"><name>B</name><department>D</department><salary>2</salary></employee>',
        ]
        result = wrap_in_root(fragments)
        doc = ET.fromstring(result)
        assert doc.tag == "employees"
        assert len(doc.findall("employee")) == 2

    def test_custom_root_tag(self):
        fragments = ['<item id="1"><name>X</name><department>D</department><salary>1</salary></item>']
        result = wrap_in_root(fragments, root_tag="items")
        doc = ET.fromstring(result)
        assert doc.tag == "items"

    def test_includes_xml_declaration(self):
        fragments = ['<employee id="1"><name>A</name><department>D</department><salary>1</salary></employee>']
        result = wrap_in_root(fragments)
        assert result.startswith("<?xml")

    def test_empty_fragments(self):
        result = wrap_in_root([])
        doc = ET.fromstring(result)
        assert doc.tag == "employees"
        assert len(list(doc)) == 0


class TestRoundTrip:
    """Tests for DataFrame → XML → DataFrame round-trip."""

    def test_round_trip_preserves_data(self, spark):
        df = spark.createDataFrame(SAMPLE_DATA)

        row_to_xml_udf = udf(
            lambda eid, n, d, s: row_to_xml(eid, n, d, s),
            StringType(),
        )
        xml_df = df.withColumn(
            "xml",
            row_to_xml_udf(F.col("emp_id"), F.col("name"), F.col("dept"), F.col("salary")),
        )

        fragments = [row["xml"] for row in xml_df.select("xml").collect()]
        full_xml = wrap_in_root(fragments)

        root = ET.fromstring(full_xml)
        round_trip_rows = []
        for emp in root.findall("employee"):
            round_trip_rows.append(
                {
                    "emp_id": int(emp.attrib["id"]),
                    "name": emp.findtext("name"),
                    "department": emp.findtext("department"),
                    "salary": int(emp.findtext("salary", default="0")),
                }
            )

        rt_df = spark.createDataFrame(round_trip_rows)
        assert rt_df.count() == len(SAMPLE_DATA)

        original_names = {r["name"] for r in df.select("name").collect()}
        rt_names = {r["name"] for r in rt_df.select("name").collect()}
        assert original_names == rt_names

    def test_filtered_round_trip(self, spark):
        df = spark.createDataFrame(SAMPLE_DATA)

        row_to_xml_udf = udf(
            lambda eid, n, d, s: row_to_xml(eid, n, d, s),
            StringType(),
        )
        xml_df = df.withColumn(
            "xml",
            row_to_xml_udf(F.col("emp_id"), F.col("name"), F.col("dept"), F.col("salary")),
        )

        eng_fragments = [row["xml"] for row in xml_df.filter(F.col("dept") == "Engineering").select("xml").collect()]
        full_xml = wrap_in_root(eng_fragments)
        root = ET.fromstring(full_xml)

        assert len(root.findall("employee")) == 3
        names = {emp.findtext("name") for emp in root.findall("employee")}
        assert names == {"Alice", "Charlie", "Eve"}

    def test_xml_column_not_null(self, spark):
        df = spark.createDataFrame(SAMPLE_DATA)
        row_to_xml_udf = udf(
            lambda eid, n, d, s: row_to_xml(eid, n, d, s),
            StringType(),
        )
        xml_df = df.withColumn(
            "xml",
            row_to_xml_udf(F.col("emp_id"), F.col("name"), F.col("dept"), F.col("salary")),
        )
        assert xml_df.filter(F.col("xml").isNull()).count() == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
