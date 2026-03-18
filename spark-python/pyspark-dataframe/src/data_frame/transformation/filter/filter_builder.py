from pyspark.sql import functions as F

class FilterBuilder:
    """Build complex filter conditions"""
    
    @staticmethod
    def range_filter(df, column, min_val=None, max_val=None):
        """Filter by range"""
        condition = F.lit(True)
        if min_val is not None:
            condition = condition & (F.col(column) >= min_val)
        if max_val is not None:
            condition = condition & (F.col(column) <= max_val)
        return df.filter(condition)
    
    @staticmethod
    def text_search(df, column, search_term, partial_match=True):
        """Search text in column"""
        if partial_match:
            return df.filter(F.col(column).contains(search_term))
        return df.filter(F.col(column) == search_term)
    
    @staticmethod
    def filter_by_list(df, column, values, exclude=False):
        """Filter by list of values"""
        if exclude:
            return df.filter(~F.col(column).isin(values))
        return df.filter(F.col(column).isin(values))
    
    @staticmethod
    def null_filter(df, column, include_nulls=False):
        """Filter null values"""
        if include_nulls:
            return df.filter(F.col(column).isNull())
        return df.filter(F.col(column).isNotNull())
    
    @staticmethod
    def complex_and_filter(df, conditions):
        """Apply multiple AND conditions"""
        final_condition = F.lit(True)
        for condition in conditions:
            final_condition = final_condition & condition
        return df.filter(final_condition)