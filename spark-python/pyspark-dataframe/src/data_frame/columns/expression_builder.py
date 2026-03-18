from pyspark.sql import functions as F

class ExpressionBuilder:
    """Build complex column expressions"""
    
    @staticmethod
    def conditional_column(condition, true_value, false_value):
        """Create conditional column"""
        return F.when(condition, true_value).otherwise(false_value)
    
    @staticmethod
    def derive_from_date(df, date_col, components):
        """Derive date components"""
        date_funcs = {
            "year": F.year,
            "month": F.month,
            "day": F.dayofmonth,
            "day_of_week": F.dayofweek,
            "quarter": F.quarter,
            "week_of_year": F.weekofyear
        }
        
        for component in components:
            if component in date_funcs:
                df = df.withColumn(f"{date_col}_{component}", 
                                   date_funcs[component](F.col(date_col)))
        return df
    
    @staticmethod
    def string_operations(df, string_col, operations):
        """Perform string operations"""
        string_funcs = {
            "upper": F.upper,
            "lower": F.lower,
            "length": F.length,
            "trim": F.trim,
            "reverse": F.reverse
        }
        
        for op_name, cols in operations.items():
            if op_name in string_funcs:
                if isinstance(cols, list):
                    for col in cols:
                        df = df.withColumn(f"{col}_{op_name}", 
                                           string_funcs[op_name](F.col(col)))
                else:
                    df = df.withColumn(f"{cols}_{op_name}", 
                                       string_funcs[op_name](F.col(cols)))
        return df
