"""Tests for reading, parsing, and writing HTML tables via the pys_html library."""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from pys_html import HtmlReader, HtmlWriter
from pys_html.parsing import explode_links, extract_links, extract_meta_tags, strip_html_tags
from pys_html.schema import link_schema, table_row_schema

SAMPLE_TABLE_HTML = """
<table>
  <tr><th>Name</th><th>Age</th></tr>
  <tr><td>Alice</td><td>30</td></tr>
  <tr><td>Bob</td><td>25</td></tr>
</table>
"""


def test_read_tables_returns_one_dataframe_per_table(spark: SparkSession) -> None:
    reader = HtmlReader(spark)
    tables = reader.read_tables(SAMPLE_TABLE_HTML)

    assert len(tables) == 1
    df = tables[0]
    assert df.count() == 2
    assert set(df.columns) == {"Name", "Age"}


def test_read_table_by_index(spark: SparkSession) -> None:
    reader = HtmlReader(spark)
    df = reader.read_table(SAMPLE_TABLE_HTML, index=0)

    rows = sorted(df.collect(), key=lambda r: r["Name"])
    assert rows[0]["Name"] == "Alice"
    assert rows[0]["Age"] == "30"


def test_read_table_from_file(spark: SparkSession, tmp_path: Path) -> None:
    html_file = tmp_path / "table.html"
    html_file.write_text(SAMPLE_TABLE_HTML)

    reader = HtmlReader(spark)
    df = reader.read_table(str(html_file))

    assert df.count() == 2


def test_read_table_with_attrs_filter(spark: SparkSession) -> None:
    html = """
    <table id="a"><tr><th>X</th></tr><tr><td>1</td></tr></table>
    <table id="b"><tr><th>Y</th></tr><tr><td>2</td></tr></table>
    """
    reader = HtmlReader(spark).attrs({"id": "b"})
    df = reader.read_table(html)

    assert df.columns == ["Y"]
    assert df.collect()[0]["Y"] == "2"


def test_read_matching_tables_merges_across_files(spark: SparkSession, tmp_path: Path) -> None:
    jan_html = """
    <table id="unrelated"><tr><th>Ignore</th></tr><tr><td>Me</td></tr></table>
    <table id="results"><tr><th>Region</th><th>Revenue</th></tr><tr><td>East</td><td>1000</td></tr></table>
    """
    feb_html = """
    <table id="results"><tr><th>Region</th><th>Revenue</th></tr><tr><td>East</td><td>1100</td></tr></table>
    """
    jan_file = tmp_path / "jan.html"
    feb_file = tmp_path / "feb.html"
    jan_file.write_text(jan_html)
    feb_file.write_text(feb_html)

    reader = HtmlReader(spark).attrs({"id": "results"})
    merged = reader.read_matching_tables([str(jan_file), str(feb_file)])

    assert set(merged.columns) == {"Region", "Revenue", "_source_file"}
    assert merged.count() == 2

    rows = sorted(merged.collect(), key=lambda r: r["Revenue"])
    assert rows[0]["Revenue"] == "1000"
    assert rows[0]["_source_file"] == str(jan_file)
    assert rows[1]["Revenue"] == "1100"
    assert rows[1]["_source_file"] == str(feb_file)


def test_read_matching_tables_without_source_column(spark: SparkSession, tmp_path: Path) -> None:
    html = "<table id='results'><tr><th>X</th></tr><tr><td>1</td></tr></table>"
    file_a = tmp_path / "a.html"
    file_a.write_text(html)

    reader = HtmlReader(spark).attrs({"id": "results"})
    merged = reader.read_matching_tables([str(file_a)], source_column=None)

    assert merged.columns == ["X"]


def test_read_matching_tables_raises_when_no_source_matches(spark: SparkSession, tmp_path: Path) -> None:
    html = "<table id='other'><tr><th>X</th></tr><tr><td>1</td></tr></table>"
    file_a = tmp_path / "a.html"
    file_a.write_text(html)

    reader = HtmlReader(spark).attrs({"id": "results"})
    with pytest.raises(ValueError, match="No matching table found"):
        reader.read_matching_tables([str(file_a)])


def test_read_matching_tables_raises_on_empty_sources(spark: SparkSession) -> None:
    reader = HtmlReader(spark)
    with pytest.raises(ValueError, match="No sources given"):
        reader.read_matching_tables([])


def test_list_tables_returns_attrs_and_shape_per_table(spark: SparkSession) -> None:
    html = """
    <table id="a" class="foo"><tr><th>X</th></tr><tr><td>1</td></tr></table>
    <table id="b" class="bar baz">
      <caption>Results</caption>
      <tr><th>Y</th><th>Z</th></tr>
      <tr><td>2</td><td>3</td></tr>
    </table>
    """
    reader = HtmlReader(spark)
    summaries = reader.list_tables(html)

    assert len(summaries) == 2

    first, second = summaries
    assert first["index"] == 0
    assert first["attrs"] == {"id": "a", "class": "foo"}
    assert first["num_rows"] == 2
    assert first["num_cols"] == 1
    assert first["headers"] == ["X"]
    assert first["caption"] is None

    assert second["index"] == 1
    assert second["attrs"] == {"id": "b", "class": "bar baz"}
    assert second["num_rows"] == 2
    assert second["num_cols"] == 2
    assert second["headers"] == ["Y", "Z"]
    assert second["caption"] == "Results"


def test_list_tables_empty_document_returns_empty_list(spark: SparkSession) -> None:
    reader = HtmlReader(spark)
    assert reader.list_tables("<html><body><p>No tables here</p></body></html>") == []


def test_read_links(spark: SparkSession) -> None:
    html = '<a href="/docs">Docs</a><a href="/blog">Blog</a>'
    reader = HtmlReader(spark)
    df = reader.read_links(html)

    rows = sorted(df.collect(), key=lambda r: r["href"])
    assert rows[0]["href"] == "/blog"
    assert rows[0]["text"] == "Blog"


def test_read_text_strips_tags(spark: SparkSession) -> None:
    reader = HtmlReader(spark)
    text = reader.read_text("<div><h1>Title</h1><p>Body</p></div>")

    assert "Title" in text
    assert "Body" in text
    assert "<" not in text


def test_extract_links_udf(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("p1", '<a href="/a">Alpha</a>')],
        schema="page STRING, html STRING",
    )
    result = df.withColumn("links", extract_links("html")).collect()[0]["links"]

    assert result[0]["text"] == "Alpha"
    assert result[0]["href"] == "/a"


def test_explode_links(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("p1", '<a href="/a">Alpha</a><a href="/b">Beta</a>')],
        schema="page STRING, html STRING",
    )
    exploded = explode_links(df, html_col="html", link_col="link")

    assert exploded.count() == 2


def test_strip_html_tags_udf(spark: SparkSession) -> None:
    df = spark.createDataFrame([("p1", "<p>Hello <b>World</b></p>")], schema="id STRING, html STRING")
    plain = df.withColumn("plain", strip_html_tags("html")).collect()[0]["plain"]

    assert plain == "Hello World"


def test_extract_meta_tags_udf(spark: SparkSession) -> None:
    html_doc = '<html><head><meta name="author" content="spark-batch"></head></html>'
    df = spark.createDataFrame([("d1", html_doc)], schema="id STRING, html STRING")
    meta = df.withColumn("meta", extract_meta_tags("html")).collect()[0]["meta"]

    assert meta["author"] == "spark-batch"


def test_html_writer_round_trip(spark: SparkSession) -> None:
    original = spark.createDataFrame([("Widget", 9.99)], schema="product STRING, price DOUBLE")
    markup = HtmlWriter().render(original)

    assert "<table" in markup
    assert "Widget" in markup

    round_tripped = HtmlReader(spark).read_table(markup)
    assert round_tripped.count() == 1


def test_html_writer_write_page(tmp_path: Path, spark: SparkSession) -> None:
    df = spark.createDataFrame([("Widget", 9.99)], schema="product STRING, price DOUBLE")
    out_file = tmp_path / "report.html"

    HtmlWriter().write_page(df, str(out_file), title="Report")

    content = out_file.read_text()
    assert "<title>Report</title>" in content
    assert "Widget" in content


def test_table_row_schema_all_strings() -> None:
    schema = table_row_schema(["Rank", "Country"])

    assert [f.name for f in schema.fields] == ["Rank", "Country"]
    assert all(f.dataType.typeName() == "string" for f in schema.fields)


def test_link_schema_fields() -> None:
    schema = link_schema()

    assert {f.name for f in schema.fields} == {"text", "href"}
