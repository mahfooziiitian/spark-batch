from pyspark.sql import DataFrame, functions as F

class RankCalculator:
    """Calculate different types of rankings"""
    
    @staticmethod
    def rank_within_partition(df, window_spec, rank_col_name="rank")-> DataFrame:
        """Calculate RANK()"""
        return df.withColumn(rank_col_name, F.rank().over(window_spec))
    
    @staticmethod
    def dense_rank_within_partition(df, window_spec, rank_col_name="dense_rank")-> DataFrame:
        """Calculate DENSE_RANK()"""
        return df.withColumn(rank_col_name, F.dense_rank().over(window_spec))
    
    @staticmethod
    def row_number_within_partition(df, window_spec, row_num_col_name="row_number")-> DataFrame:
        """Calculate ROW_NUMBER()"""
        return df.withColumn(row_num_col_name, F.row_number().over(window_spec))
    
    @staticmethod
    def percent_rank_within_partition(df, window_spec, pct_col_name="percent_rank")-> DataFrame:
        """Calculate PERCENT_RANK()"""
        return df.withColumn(pct_col_name, F.percent_rank().over(window_spec))
    
    @staticmethod
    def ntile_within_partition(df, window_spec, n, ntile_col_name="ntile") -> DataFrame:
        """Calculate NTILE(n)"""
        return df.withColumn(ntile_col_name, F.ntile(n).over(window_spec))
