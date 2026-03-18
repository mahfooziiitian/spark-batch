
class Joiner:
    """Perform various join operations"""
    
    @staticmethod
    def inner_join(df1, df2, join_cols):
        """Perform INNER JOIN"""
        return df1.join(df2, on=join_cols, how="inner")
    
    @staticmethod
    def left_join(df1, df2, join_cols):
        """Perform LEFT JOIN"""
        return df1.join(df2, on=join_cols, how="left")
    
    @staticmethod
    def right_join(df1, df2, join_cols):
        """Perform RIGHT JOIN"""
        return df1.join(df2, on=join_cols, how="right")
    
    @staticmethod
    def full_outer_join(df1, df2, join_cols):
        """Perform FULL OUTER JOIN"""
        return df1.join(df2, on=join_cols, how="full")
    
    @staticmethod
    def cross_join(df1, df2):
        """Perform CROSS JOIN"""
        return df1.crossJoin(df2)
    
    @staticmethod
    def join_with_condition(df1, df2, condition, join_type="inner"):
        """Join with a custom condition"""
        return df1.join(df2, on=condition, how=join_type)
    
    @staticmethod
    def broadcast_join(df1, df2, join_cols, join_type="inner"):
        """Perform BROADCAST JOIN for small tables"""
        from pyspark.sql.functions import broadcast
        return df1.join(broadcast(df2), on=join_cols, how=join_type)