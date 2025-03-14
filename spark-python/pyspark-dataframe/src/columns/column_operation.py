from datetime import datetime

from pyspark.sql.functions import lit, to_date, to_timestamp
from pyspark.sql.types import IntegerType

from constant import constant


def add_audit_col(df_src):
    """
    Add audit columns to the DataFrame.

    Args:
        df_src (DataFrame): The DataFrame to which audit columns need to be added.

    Returns:
        DataFrame: The DataFrame with added audit columns. tyuio
    """
    now = datetime.now()

    return (
        df_src.withColumn("crt_user_id", lit(constant.USER))
        .withColumn("crt_dttm", lit(now))
        .withColumn("upd_user_id", lit(constant.USER))
        .withColumn("upd_dttm", lit(now))
    )


def convert_date_columns(
        df_src, date_col_list, timestamp_col_list, int_col_list, source_date_format
):
    """
    Convert specified columns to appropriate data types.

    Args:
        df_src (DataFrame): The DataFrame to perform column conversions on.
        date_col_list (list): List of column names to convert to date type.
        timestamp_col_list (list): List of column names to convert to timestamp type.
        int_col_list (list): List of column names to convert to integer type.
        source_date_format (str): The format of source date columns.

    Returns:
        DataFrame: The DataFrame with converted columns. testyuu
    """
    for col_name in date_col_list:
        df_src = df_src.withColumn(col_name, to_date(col_name, source_date_format))

    for col_name in timestamp_col_list:
        df_src = df_src.withColumn(col_name, to_timestamp(col_name, source_date_format))

    if int_col_list:
        for col_name in int_col_list:
            df_src = df_src.withColumn(col_name, df_src[col_name].cast(IntegerType()))

    return df_src
