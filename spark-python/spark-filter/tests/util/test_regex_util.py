from pyspark.sql.functions import col

from util.regex_util import replace_filter_query_expression
import pytest


def test_expect_column_values_to_not_match_regex_list(spark):
    key = "expect_column_values_to_not_match_regex_list"
    param = {
        "regex_list": ["^\\d{3}/\\d{2}$", "^\\d{1,3}/\\d{1,3}$"],
        "column": "Blood_Pressure",
        "result_format": {
            "return_unexpected_index_query": True,
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
    }

    filter_query = replace_filter_query_expression(key, param)
    expected_outcome = col("Blood_Pressure").rlike("^\\d{3}/\\d{2}$|^\\d{1,3}/\\d{1,3}$")
    comparision_result = str(filter_query) == str(expected_outcome)

    assert comparision_result


def test_expect_column_values_to_match_regex(spark):
    key = "expect_column_values_to_match_regex"
    param = {
        "regex": "^\\d{3}/\\d{2}$",
        "column": "Blood_Pressure",
        "result_format": {
            "return_unexpected_index_query": True,
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
    }

    filter_query = replace_filter_query_expression(key, param)
    expected_outcome = ~col("Blood_Pressure").rlike("^\\d{3}/\\d{2}$")
    comparision_result = str(filter_query) == str(expected_outcome)

    assert comparision_result


def test_expect_multicolumn_sum_to_equal(spark):
    key = "expect_multicolumn_sum_to_equal"
    param = {
        "column_list": ["load_cd", "transform_cd"],
        "sum_total": 0,
        "mostly": 0.8,
        "result_format": {
            "return_unexpected_index_query": True,
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
    }
    filter_query = replace_filter_query_expression(key, param)
    expected_outcome = col("Blood_Pressure").rlike("^I.*$")
    comparision_result = str(filter_query) == str(expected_outcome)
    assert comparision_result


def test_expect_column_values_to_not_match_regex(spark):
    key = "expect_column_values_to_not_match_regex"
    param = {
        "regex": "^I.*$",
        "column": "Blood_Pressure",
        "result_format": {
            "return_unexpected_index_query": True,
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
    }

    filter_query = replace_filter_query_expression(key, param)
    expected_outcome = col("Blood_Pressure").rlike("^I.*$")
    comparision_result = str(filter_query) == str(expected_outcome)
    assert comparision_result


def test_expect_column_values_to_match_regex_list(spark):
    key = "expect_column_values_to_match_regex_list"
    param = {
        "regex_list": ["^\\d{3}/\\d{2}$", "^\\d{1,3}/\\d{1,3}$"],
        "column": "Blood_Pressure",
        "result_format": {
            "return_unexpected_index_query": True,
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
    }

    filter_query = replace_filter_query_expression(key, param)
    expected_outcome = ~col("Blood_Pressure").rlike("^\\d{3}/\\d{2}$|^\\d{1,3}/\\d{1,3}$")
    comparision_result = str(filter_query) == str(expected_outcome)

    assert comparision_result
