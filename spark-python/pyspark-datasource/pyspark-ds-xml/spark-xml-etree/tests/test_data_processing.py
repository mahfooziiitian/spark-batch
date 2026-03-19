"""Tests for single-field and multi-field XML extraction via UDFs."""

import xml.etree.ElementTree as ET

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from spark_etree.xmls_data_processing import SAMPLE_XML, extract_title
from spark_etree.xmls_data_processing_multiple_column import (
    CD_INFO_SCHEMA,
    extract_cd_info,
    select_text,
)


class TestExtractTitle:
    """Tests for xmls_data_processing.extract_title."""

    def test_extracts_title_from_valid_cd(self):
        xml = "<CD><TITLE>Empire Burlesque</TITLE><ARTIST>Bob Dylan</ARTIST></CD>"
        assert extract_title(xml) == "Empire Burlesque"

    def test_returns_none_when_title_missing(self):
        xml = "<CD><ARTIST>Bob Dylan</ARTIST></CD>"
        assert extract_title(xml) is None

    def test_returns_first_title_when_multiple(self):
        xml = "<CD><TITLE>First</TITLE><TITLE>Second</TITLE></CD>"
        assert extract_title(xml) == "First"

    def test_extract_title_udf_across_dataframe(self, spark):
        catalog = ET.fromstring(SAMPLE_XML)
        cd_strings = [ET.tostring(cd, encoding="unicode").strip() for cd in catalog.findall("CD")]
        rows = [Row(index=i, cd=xml) for i, xml in enumerate(cd_strings)]
        df = spark.createDataFrame(rows)

        extract_title_udf = udf(extract_title, StringType())
        result = df.select("index", extract_title_udf(F.col("cd")).alias("title")).orderBy("index").collect()

        assert len(result) == 5
        assert result[0]["title"] == "Empire Burlesque"
        assert result[1]["title"] == "Hide your heart"
        assert result[4]["title"] == "Eros"


class TestSelectText:
    """Tests for xmls_data_processing_multiple_column.select_text."""

    def test_returns_text_for_existing_element(self):
        doc = ET.fromstring("<CD><TITLE>Test</TITLE></CD>")
        assert select_text(doc, "TITLE") == "Test"

    def test_returns_none_for_missing_element(self):
        doc = ET.fromstring("<CD><ARTIST>Someone</ARTIST></CD>")
        assert select_text(doc, "TITLE") is None

    def test_returns_none_for_empty_element(self):
        doc = ET.fromstring("<CD><TITLE></TITLE></CD>")
        assert select_text(doc, "TITLE") is None


class TestExtractCdInfo:
    """Tests for xmls_data_processing_multiple_column.extract_cd_info."""

    def test_extracts_all_fields(self):
        xml = (
            "<CD><TITLE>Greatest Hits</TITLE><ARTIST>Dolly Parton</ARTIST><COUNTRY>USA</COUNTRY><YEAR>1982</YEAR></CD>"
        )
        info = extract_cd_info(xml)
        assert info == {
            "title": "Greatest Hits",
            "artist": "Dolly Parton",
            "country": "USA",
            "year": "1982",
        }

    def test_missing_fields_return_none(self):
        xml = "<CD><TITLE>Only Title</TITLE></CD>"
        info = extract_cd_info(xml)
        assert info["title"] == "Only Title"
        assert info["artist"] is None
        assert info["country"] is None
        assert info["year"] is None

    def test_struct_udf_schema(self, spark):
        catalog = ET.fromstring(SAMPLE_XML)
        cd_strings = [ET.tostring(cd, encoding="unicode").strip() for cd in catalog.findall("CD")]
        rows = [Row(index=i, cd=xml) for i, xml in enumerate(cd_strings)]
        df = spark.createDataFrame(rows)

        extract_udf = udf(extract_cd_info, CD_INFO_SCHEMA)
        result = (
            df.withColumn("info", extract_udf("cd"))
            .select("index", "info.title", "info.artist", "info.country", "info.year")
            .orderBy("index")
            .collect()
        )

        assert len(result) == 5
        assert result[0]["title"] == "Empire Burlesque"
        assert result[0]["artist"] == "Bob Dylan"
        assert result[0]["country"] == "USA"
        assert result[0]["year"] == "1985"
        assert result[3]["country"] == "UK"

    def test_sample_xml_has_five_cds(self):
        catalog = ET.fromstring(SAMPLE_XML)
        assert len(catalog.findall("CD")) == 5


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
