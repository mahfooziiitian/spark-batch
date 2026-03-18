class SchemaAnalyzer:
    """Analyze DataFrame schemas and structure"""

    @staticmethod
    def print_schema_details(df):
        """Print detailed schema information"""
        print("=" * 50)
        print("SCHEMA DETAILS")
        print("=" * 50)
        df.printSchema()

    @staticmethod
    def get_column_stats(df):
        """Get statistics for each column"""
        stats = {}
        for col_name in df.columns:
            col_type = dict(df.dtypes)[col_name]
            stats[col_name] = {
                "type": col_type,
                "nullable": df.schema[col_name].nullable,
                "metadata": df.schema[col_name].metadata,
            }
        return stats

    @staticmethod
    def sample_data(df, n=5):
        """Display sample data"""
        print("=" * 50)
        print(f"SAMPLE DATA (First {n} rows)")
        print("=" * 50)
        df.show(n, truncate=False)
