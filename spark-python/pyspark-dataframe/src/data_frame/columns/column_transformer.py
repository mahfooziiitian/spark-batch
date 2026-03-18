from pyspark.sql import functions as F

class ColumnTransformer:
    """Perform column operations"""
    
    @staticmethod
    def add_columns(df, **kwargs):
        """Add multiple columns"""
        for col_name, value in kwargs.items():
            df = df.withColumn(col_name, F.lit(value))
        return df
    
    @staticmethod
    def rename_columns(df, rename_map):
        """Rename multiple columns"""
        for old_name, new_name in rename_map.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df
    
    @staticmethod
    def select_columns(df, columns, drop=False):
        """Select or drop columns"""
        if drop:
            return df.drop(*columns)
        return df.select(*columns)
    
    @staticmethod
    def cast_columns(df, cast_map):
        """Cast columns to different types"""
        for col_name, new_type in cast_map.items():
            df = df.withColumn(col_name, F.col(col_name).cast(new_type))
        return df

