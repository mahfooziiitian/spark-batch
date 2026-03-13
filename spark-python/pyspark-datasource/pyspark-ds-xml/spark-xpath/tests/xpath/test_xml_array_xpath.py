# test_xml_processing.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("XML Processing Test")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_xpath_string_extracts_header_fields(spark):
    """Extract individual header fields with xpath_string."""
    data = [
        "<Msg><Header><tag1>some str1</tag1><tag2>2</tag2>"
        "<tag3>2022-02-16 10:39:26.730</tag3></Header>"
        "<Body><Pair><N>N1</N><V>V1</V></Pair></Body></Msg>",
    ]

    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_xml_header")

    result = spark.sql(
        "SELECT xpath_string(data, 'Msg/Header/tag1') AS tag1 FROM test_xml_header"
    )
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0].tag1 == "some str1"


def test_xpath_string_wildcard(spark):
    """Wildcard xpath returns first child text."""
    data = [
        "<Msg><Header><tag1>first</tag1><tag2>second</tag2></Header></Msg>",
    ]

    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_xml_wildcard")

    result = spark.sql(
        "SELECT xpath_string(data, 'Msg/Header/*') AS val FROM test_xml_wildcard"
    )
    rows = result.collect()

    assert rows[0].val == "first"


def test_xpath_array_extraction(spark):
    """xpath() returns an array of matching text nodes."""
    data = [
        "<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>",
    ]

    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "x")
    df.createOrReplaceTempView("test_xml_array")

    result = spark.sql(
        "SELECT xpath(x, 'a/b/text()') AS items FROM test_xml_array"
    )
    rows = result.collect()

    assert rows[0].items == ["b1", "b2", "b3"]


def test_xpath_boolean(spark):
    """xpath_boolean evaluates a condition inside the XML."""
    data = [
        "<root><item><score>10</score></item></root>",
        "<root><item><score>3</score></item></root>",
    ]

    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_xml_bool")

    result = spark.sql(
        "SELECT xpath_boolean(data, 'root/item[score >= 5]') AS high "
        "FROM test_xml_bool"
    )
    rows = result.collect()

    assert rows[0].high is True
    assert rows[1].high is False


def test_xpath_with_namespaced_xml(spark):
    """Namespace prefixes are stripped — use local element names."""
    data = [
        '<ns0:Root xmlns:ns0="http://example.com">'
        "<Child>hello</Child></ns0:Root>",
    ]

    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_xml_ns")

    result = spark.sql(
        "SELECT xpath_string(data, 'Root/Child') AS val FROM test_xml_ns"
    )
    rows = result.collect()

    assert rows[0].val == "hello"


def test_xpath_multiple_rows(spark):
    """XPath works across multiple DataFrame rows."""
    data = [
        "<Msg><Header><tag1>row1</tag1></Header></Msg>",
        "<Msg><Header><tag1>row2</tag1></Header></Msg>",
        "<Msg><Header><tag1>row3</tag1></Header></Msg>",
    ]

    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_xml_multi")

    result = spark.sql(
        "SELECT xpath_string(data, 'Msg/Header/tag1') AS tag1 FROM test_xml_multi"
    )
    tags = sorted([row.tag1 for row in result.collect()])

    assert tags == ["row1", "row2", "row3"]
