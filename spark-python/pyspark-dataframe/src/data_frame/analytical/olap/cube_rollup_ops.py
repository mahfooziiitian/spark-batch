from pyspark.sql import functions as F

class CubeRollupOps:
    """Perform cube and rollup operations"""
    
    @staticmethod
    def cube_aggregation(df, dimensions, metrics):
        """Perform CUBE operation"""
        return df.cube(*dimensions).agg(*metrics)
    
    @staticmethod
    def rollup_aggregation(df, dimensions, metrics):
        """Perform ROLLUP operation"""
        return df.rollup(*dimensions).agg(*metrics)
    
    @staticmethod
    def grouping_sets_aggregation(df, grouping_sets, metrics):
        """Perform GROUPING SETS operation"""
        # Spark doesn't have direct GROUPING SETS, simulate with union of cubes
        results = []
        for group_set in grouping_sets:
            if group_set:
                result = df.groupBy(*group_set).agg(*metrics)
            else:
                # Empty group set means overall total
                result = df.agg(*[m.alias(f"{m}_total") for m in metrics])
            results.append(result)
        
        # Union all results
        from functools import reduce
        return reduce(lambda df1, df2: df1.unionAll(df2), results)
