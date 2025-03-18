import json
import os
import re
import sys

from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import col, lit, stddev, mean

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]

filter_mapping = {
    "expect_column_values_to_be_null": "col('{{column}}').isNotNull()",
    "expect_column_values_to_be_in_set": "~col('{{column}}').isin({{value_set}})",
    "expect_column_distinct_values_to_equal_set": "col('{{column}}').isin({{value_set}})",
    "expect_column_values_to_not_be_in_set": "col('{{column}}').isin({{value_set}})",
    "expect_column_values_to_not_be_null": "col('{{column}}').isNull()",
    "expect_column_values_to_be_of_type": "col('{{column}}').cast({{type_}}).isNull()",
    "expect_column_pair_values_a_to_be_greater_than_b": "col('{{column_A}}') <= col('{{column_B}}')",
    "expect_column_pair_values_to_be_in_set": "",
    "expect_column_pair_values_to_be_equal": "col('{{column_A}}') != col('{{column_B}}')",
    "expect_column_value_lengths_to_equal": "length(col('{{column}}')) != {{value}}",
    "expect_column_value_lengths_to_be_between": "~(length(col('{{column}}')) >= {{min_value}}) & (length(col('{{column}}')) <= {{max_value}})",
    "expect_column_values_to_be_between": "(col('{{column}}') >= {{min_value}}) & (col('{{column}}') <= {{max_value}})",
    "expect_column_values_to_match_regex": "~col('{{column}}').rlike('{{regex}}')",
    "expect_column_values_to_not_match_regex": "col('{{column}}').rlike('{{regex}}')",
    "expect_column_values_to_be_unique": "df.groupBy('{{column}}').count().filter(col('count') > 1)",
    "expect_column_values_to_match_regex_list": "~col('{{column}}').rlike('|'.join({{regex_list}}))",
    "expect_column_values_to_not_match_regex_list": "col('{{column}}').rlike('|'.join({{regex_list}}))",
    "expect_multicolumn_sum_to_equal": "(sum(col(coll) for coll in {{column_list}})) >= lit({{sum_total}})",
    "expect_column_value_z_scores_to_be_between": "((col('{{column}}') - mean('{{column}}')) / stddev('{{column}}')) >= '{{threshold}}'",
}


def raw_string(s: str):
    return r"{}".format(s)


def replace_filter_query_expression(key: str, param: dict[str, str]):
    filter_expr_template = re.sub(
        r"\{\{(\w+)\}\}",
        lambda m: (
            json.dumps(param[m.group(1)])
            if isinstance(param.get(m.group(1)), list)
            else raw_string(param.get(m.group(1), ""))
        ),
        filter_mapping.get(key, ""),
    )
    print("========Start filter_expr_template======================")
    print(f"filter_expr_template: {filter_expr_template}")
    print("========End filter_expr_template========================")

    print("========Start filter_expr===============================")
    filter_expr = eval(filter_expr_template)
    print(f"filter_expr: {filter_expr}")
    if isinstance(filter_expr, str):
        filter_expr = eval(filter_expr)
        print(f"filter_expr: {filter_expr}")
    print("========End filter_expr=================================")

    return filter_expr


def select_filter_columns(prams: dict[str, str]):
    """
    Function Name: select_filter_columns

    Description:

        This function selects the columns to be used for filtering the records based on the provided parameters.

    Parameters:

        prams (dict): A dictionary containing the parameters to be used for filtering the records.

    Returns:

        filter_columns (list): A list of columns to be used for filtering the records.
    """
    FILTER_COLUMN_LIST = ["column", "column_A", "column_B", "column_list"]
    filter_columns = []
    for key in prams.keys():
        if key in FILTER_COLUMN_LIST:
            if isinstance(prams[key], list):
                filter_columns.extend(prams[key])
            else:
                filter_columns.append(prams[key])
    return filter_columns


def main():
    spark = SparkSession.builder.appName("sparksession").getOrCreate()
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
    print(filter_query)
    data = [(5, -10), (10, -20), (39, 30)]
    df = spark.createDataFrame(data, ["load_cd", "transform_cd"])

    # Filter the DataFrame using rlike()
    filtered_df = df.filter(filter_query)
    # Show the filtered DataFrame
    filtered_df.show()
    working_filter = (col("load_cd") + col("transform_cd")) >= lit(0)
    selected_columns = select_filter_columns(param)
    filtered_df = df.filter(working_filter).select(selected_columns)
    filtered_df.show()


if __name__ == "__main__":
    main()
