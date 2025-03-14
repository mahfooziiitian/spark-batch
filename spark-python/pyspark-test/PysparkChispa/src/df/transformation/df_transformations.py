def modify_column_names(df, fun):
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, fun(col_name))
    return df

