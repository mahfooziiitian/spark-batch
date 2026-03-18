class SetOperator:
    """Perform set operations on DataFrames"""
    
    @staticmethod
    def union_all(df1, df2):
        """UNION ALL operation (keeps duplicates)"""
        return df1.unionAll(df2)
    
    @staticmethod
    def union_distinct(df1, df2):
        """UNION operation (removes duplicates)"""
        return df1.union(df2).distinct()
    
    @staticmethod
    def intersect(df1, df2):
        """INTERSECT operation"""
        return df1.intersect(df2)
    
    @staticmethod
    def except_df(df1, df2):
        """EXCEPT/MINUS operation"""
        return df1.exceptAll(df2)
    
    @staticmethod
    def union_multiple(dfs):
        """Union multiple DataFrames"""
        from functools import reduce
        return reduce(lambda df1, df2: df1.unionAll(df2), dfs)
