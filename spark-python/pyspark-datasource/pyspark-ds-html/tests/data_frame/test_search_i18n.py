"""Tests for the lightweight semantic (fuzzy) search and bilingual header helpers."""

import pytest
from pyspark.sql import SparkSession

from pys_html import bilingual_header, semantic_search
from pys_html.i18n import EXTRA_HEADER_LABELS, HINDI_TO_ENGLISH_HEADERS, has_devanagari, transliterate_devanagari


def test_bilingual_header_known_hindi_column() -> None:
    assert bilingual_header("रैयत का नाम") == "रैयत का नाम / Tenant Name"
    assert bilingual_header("क्रम संख्या") == "क्रम संख्या / Serial No."


def test_bilingual_header_known_technical_column() -> None:
    assert bilingual_header("_source_file") == "Source File"
    assert bilingual_header("_score") == "Match Score"


def test_bilingual_header_unknown_column_passthrough() -> None:
    assert bilingual_header("some_unmapped_column") == "some_unmapped_column"


def test_hindi_and_extra_header_maps_have_no_overlap() -> None:
    assert set(HINDI_TO_ENGLISH_HEADERS) & set(EXTRA_HEADER_LABELS) == set()


def test_has_devanagari() -> None:
    assert has_devanagari("रफीक")
    assert has_devanagari("mixed रफीक text")
    assert not has_devanagari("RAFIK")
    assert not has_devanagari("")


def test_transliterate_devanagari_converts_to_latin_script() -> None:
    result = transliterate_devanagari("रफीक")
    assert result != "रफीक"
    assert result.isascii()


def test_transliterate_devanagari_passthrough_for_latin_text() -> None:
    assert transliterate_devanagari("RAFIK") == "RAFIK"


def test_semantic_search_ranks_close_matches_highest(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("लक्ष्मी राय",), ("लक्षण राय",), ("रौशन परवीन",)],
        ["रैयत का नाम"],
    )

    results = semantic_search(df, column="रैयत का नाम", query="लक्ष्मी", top_k=10, min_score=0.0)

    rows = results.orderBy(results["_score"].desc()).collect()
    assert rows[0]["रैयत का नाम"] == "लक्ष्मी राय"


def test_semantic_search_is_case_insensitive(spark: SparkSession) -> None:
    df = spark.createDataFrame([("RAFIK",), ("SADIK",), ("KHADIJA",)], ["name"])

    results = semantic_search(df, column="name", query="Rafik", top_k=1, min_score=0.0)

    row = results.collect()[0]
    assert row["name"] == "RAFIK"
    assert row["_score"] == pytest.approx(1.0)


def test_semantic_search_respects_min_score_and_top_k(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("लक्ष्मी राय",), ("लक्ष्मी दास",), ("पूरी तरह अलग नाम",)],
        ["name"],
    )

    results = semantic_search(df, column="name", query="लक्ष्मी", top_k=1, min_score=0.5)

    assert results.count() == 1
    assert results.collect()[0]["name"] in {"लक्ष्मी राय", "लक्ष्मी दास"}


def test_semantic_search_custom_score_column_name(spark: SparkSession) -> None:
    df = spark.createDataFrame([("abc",), ("xyz",)], ["value"])

    results = semantic_search(df, column="value", query="abc", score_column="similarity")

    assert "similarity" in results.columns
    assert "_score" not in results.columns


def test_semantic_search_raises_on_missing_column(spark: SparkSession) -> None:
    df = spark.createDataFrame([("abc",)], ["value"])

    with pytest.raises(ValueError, match="not found in DataFrame"):
        semantic_search(df, column="does_not_exist", query="abc")


def test_semantic_search_partial_ratio_finds_query_as_substring(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("MOHAMMAD FUL HASN URF RAFI ALAM",), ("SOME UNRELATED NAME",)],
        ["name"],
    )

    ratio_results = semantic_search(df, column="name", query="Rafi Alam", top_k=1, min_score=0.0, scorer="ratio")
    partial_results = semantic_search(
        df, column="name", query="Rafi Alam", top_k=1, min_score=0.0, scorer="partial_ratio"
    )

    assert partial_results.collect()[0]["_score"] > ratio_results.collect()[0]["_score"]
    assert partial_results.collect()[0]["_score"] == pytest.approx(1.0)


def test_semantic_search_token_set_ratio_ignores_word_order_and_extra_tokens(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("1.MD AKBAR ALI 2.AJHAR ALI",), ("SOME UNRELATED NAME",)],
        ["name"],
    )

    results = semantic_search(df, column="name", query="AKBAR ALI", top_k=1, min_score=0.0, scorer="token_set_ratio")

    assert results.collect()[0]["_score"] == pytest.approx(1.0)


def test_semantic_search_raises_on_unknown_scorer(spark: SparkSession) -> None:
    df = spark.createDataFrame([("abc",)], ["value"])

    with pytest.raises(ValueError, match="Unknown scorer"):
        semantic_search(df, column="value", query="abc", scorer="does_not_exist")


def test_semantic_search_matches_devanagari_query_against_latin_script_value(spark: SparkSession) -> None:
    df = spark.createDataFrame([("RAFIK",), ("SADIK",), ("KHADIJA",)], ["name"])

    results = semantic_search(df, column="name", query="रफीक", top_k=1, min_score=0.0)

    assert results.collect()[0]["name"] == "RAFIK"


def test_semantic_search_matches_latin_query_against_devanagari_value(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("लक्ष्मी राय",), ("लक्षण राय",), ("रौशन परवीन",)],
        ["name"],
    )

    results = semantic_search(df, column="name", query="lakshmi raya", top_k=1, min_score=0.0)

    assert results.collect()[0]["name"] == "लक्ष्मी राय"
