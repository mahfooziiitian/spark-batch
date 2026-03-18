from pyspark.sql import functions as F

class MovingAnalyzer:
    """Calculate moving/rolling statistics"""
    
    @staticmethod
    def moving_average(df, window_spec, value_col, window_size, ma_col_name="moving_avg"):
        """Calculate moving average"""
        return df.withColumn(ma_col_name, F.avg(value_col).over(window_spec))
    
    @staticmethod
    def moving_sum(df, window_spec, value_col, window_size, ms_col_name="moving_sum"):
        """Calculate moving sum"""
        return df.withColumn(ms_col_name, F.sum(value_col).over(window_spec))
    
    @staticmethod
    def lag_analysis(df, window_spec, value_col, lag_count=1, lag_col_name="lag_value"):
        """Calculate lag values"""
        return df.withColumn(lag_col_name, F.lag(value_col, lag_count).over(window_spec))
    
    @staticmethod
    def lead_analysis(df, window_spec, value_col, lead_count=1, lead_col_name="lead_value"):
        """Calculate lead values"""
        return df.withColumn(lead_col_name, F.lead(value_col, lead_count).over(window_spec))
    
    @staticmethod
    def cumulative_distribution(df, window_spec, value_col, cdf_col_name="cdf"):
        """Calculate cumulative distribution"""
        return df.withColumn(cdf_col_name, F.cume_dist().over(window_spec))
    
    @staticmethod
    def first_last_value(df, window_spec, value_col, first_col="first_value", 
                         last_col="last_value"):
        """Get first and last values in window"""
        df = df.withColumn(first_col, F.first(value_col).over(window_spec))
        df = df.withColumn(last_col, F.last(value_col).over(window_spec))
        return df
