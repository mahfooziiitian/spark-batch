"""Tests for namespace-prefixed XML parsing."""

import xml.etree.ElementTree as ET

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

from spark_etree.xmls_namespace_handling import (
    BOOK_SCHEMA,
    NS,
    SAMPLE_XML,
    extract_book,
    extract_review_ratings,
)


class TestExtractBook:
    """Tests for extract_book pure function with namespaced XML."""

    def _make_book_xml(self, isbn="978-0-00-000000-0", title="Test", author="Author", year="2020"):
        for prefix, uri in NS.items():
            ET.register_namespace(prefix, uri)
        xml = (
            f'<bk:book xmlns:bk="http://example.com/books" '
            f'xmlns:rv="http://example.com/reviews" isbn="{isbn}">'
            f"<bk:title>{title}</bk:title>"
            f"<bk:author>{author}</bk:author>"
            f"<bk:year>{year}</bk:year>"
            f"<rv:reviews />"
            f"</bk:book>"
        )
        return xml

    def test_extracts_all_fields(self):
        xml = self._make_book_xml(isbn="978-1-23-456789-0", title="My Book", author="Jane", year="2024")
        info = extract_book(xml)
        assert info["isbn"] == "978-1-23-456789-0"
        assert info["title"] == "My Book"
        assert info["author"] == "Jane"
        assert info["year"] == "2024"

    def test_missing_isbn_returns_none(self):
        xml = (
            '<bk:book xmlns:bk="http://example.com/books" '
            'xmlns:rv="http://example.com/reviews">'
            "<bk:title>No ISBN</bk:title>"
            "<bk:author>Author</bk:author>"
            "<bk:year>2020</bk:year>"
            "</bk:book>"
        )
        info = extract_book(xml)
        assert info["isbn"] is None
        assert info["title"] == "No ISBN"

    def test_sample_xml_contains_three_books(self):
        for prefix, uri in NS.items():
            ET.register_namespace(prefix, uri)
        library = ET.fromstring(SAMPLE_XML)
        books = library.findall("bk:book", NS)
        assert len(books) == 3


class TestExtractReviewRatings:
    """Tests for extract_review_ratings with namespaced XML."""

    def _make_book_with_reviews(self, ratings):
        reviews = "".join(
            f"<rv:review><rv:rating>{r}</rv:rating><rv:comment>ok</rv:comment></rv:review>" for r in ratings
        )
        return (
            '<bk:book xmlns:bk="http://example.com/books" '
            'xmlns:rv="http://example.com/reviews" isbn="000">'
            "<bk:title>T</bk:title>"
            f"<rv:reviews>{reviews}</rv:reviews>"
            "</bk:book>"
        )

    def test_extracts_multiple_ratings(self):
        xml = self._make_book_with_reviews([5, 3, 4])
        assert extract_review_ratings(xml) == [5, 3, 4]

    def test_single_rating(self):
        xml = self._make_book_with_reviews([4])
        assert extract_review_ratings(xml) == [4]

    def test_no_reviews_returns_empty(self):
        xml = (
            '<bk:book xmlns:bk="http://example.com/books" '
            'xmlns:rv="http://example.com/reviews" isbn="000">'
            "<bk:title>T</bk:title>"
            "<rv:reviews />"
            "</bk:book>"
        )
        assert extract_review_ratings(xml) == []


class TestNamespaceUDFIntegration:
    """Integration tests running namespace UDFs through Spark."""

    def test_book_udf_returns_correct_schema(self, spark):
        for prefix, uri in NS.items():
            ET.register_namespace(prefix, uri)

        library = ET.fromstring(SAMPLE_XML)
        book_strings = [ET.tostring(b, encoding="unicode").strip() for b in library.findall("bk:book", NS)]
        rows = [Row(index=i, xml=xml) for i, xml in enumerate(book_strings)]
        df = spark.createDataFrame(rows)

        extract_book_udf = udf(extract_book, BOOK_SCHEMA)
        result = (
            df.withColumn("book", extract_book_udf("xml"))
            .select("book.isbn", "book.title", "book.author", "book.year")
            .orderBy("book.year")
            .collect()
        )

        assert len(result) == 3
        assert result[0]["title"] in ("JavaScript: The Good Parts", "Clean Code")
        assert result[2]["year"] == "2019"

    def test_ratings_explode_total_count(self, spark):
        for prefix, uri in NS.items():
            ET.register_namespace(prefix, uri)

        library = ET.fromstring(SAMPLE_XML)
        book_strings = [ET.tostring(b, encoding="unicode").strip() for b in library.findall("bk:book", NS)]
        rows = [Row(index=i, xml=xml) for i, xml in enumerate(book_strings)]
        df = spark.createDataFrame(rows)

        extract_ratings_udf = udf(extract_review_ratings, ArrayType(StringType()))
        exploded = df.withColumn("ratings", extract_ratings_udf("xml")).select(F.explode("ratings").alias("rating"))
        # Book 1: 2 reviews, Book 2: 1 review, Book 3: 3 reviews → 6 total
        assert exploded.count() == 6


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
