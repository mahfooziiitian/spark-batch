from pyspark.sql import functions as F

class Aggregator:
    """Perform various aggregations"""
    
    @staticmethod
    def basic_aggregations(df, group_by_cols, agg_dict):
        """Perform basic aggregations"""
        agg_exprs = []
        for col, aggs in agg_dict.items():
            for agg_name in aggs:
                if agg_name == "count":
                    agg_exprs.append(F.count(col).alias(f"{col}_count"))
                elif agg_name == "sum":
                    agg_exprs.append(F.sum(col).alias(f"{col}_sum"))
                elif agg_name == "avg":
                    agg_exprs.append(F.avg(col).alias(f"{col}_avg"))
                elif agg_name == "min":
                    agg_exprs.append(F.min(col).alias(f"{col}_min"))
                elif agg_name == "max":
                    agg_exprs.append(F.max(col).alias(f"{col}_max"))
        
        return df.groupBy(*group_by_cols).agg(*agg_exprs)
    
    @staticmethod
    def multiple_aggregations(df, group_by_cols, metrics):
        """Apply multiple aggregation functions to multiple columns"""
        agg_exprs = []
        for metric, cols in metrics.items():
            for col in cols:
                agg_exprs.append(getattr(F, metric)(col).alias(f"{col}_{metric}"))
        
        return df.groupBy(*group_by_cols).agg(*agg_exprs)
    
    @staticmethod
    def conditional_aggregations(df, group_by_cols, conditions, agg_col, agg_func="sum"):
        """Aggregate with conditions"""
        agg_exprs = []
        for condition_name, condition in conditions.items():
            filtered_col = F.when(condition, F.col(agg_col)).otherwise(0)
            if agg_func == "sum":
                agg_expr = F.sum(filtered_col).alias(f"{agg_col}_{condition_name}")
            elif agg_func == "count":
                agg_expr = F.count(F.when(condition, True)).alias(f"count_{condition_name}")
            agg_exprs.append(agg_expr)
        
        return df.groupBy(*group_by_cols).agg(*agg_exprs)