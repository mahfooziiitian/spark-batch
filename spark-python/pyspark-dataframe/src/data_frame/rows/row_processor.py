
class RowProcessor:
    """Process rows in DataFrames"""
    
    @staticmethod
    def distinct_rows(df, columns=None):
        """Get distinct rows"""
        if columns:
            return df.select(*columns).distinct()
        return df.distinct()
    
    @staticmethod
    def drop_duplicates(df, subset=None, keep='first'):
        """Drop duplicate rows"""
        return df.dropDuplicates(subset=subset)
    
    @staticmethod
    def sample_rows(df, fraction=0.1, seed=42):
        """Sample random rows"""
        return df.sample(fraction=fraction, seed=seed)
    
    @staticmethod
    def limit_rows(df, n):
        """Limit number of rows"""
        return df.limit(n)
    
    @staticmethod
    def filter_by_row_condition(df, condition):
        """Filter rows by condition"""
        return df.filter(condition)