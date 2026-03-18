
class SQLExecutor:
    """Execute complex SQL queries"""
    
    @staticmethod
    def execute_query(spark, query, params=None):
        """Execute SQL query with optional parameters"""
        if params:
            # Replace placeholders
            for key, value in params.items():
                if isinstance(value, str):
                    query = query.replace(f"${{{key}}}", f"{value}")
                else:
                    query = query.replace(f"${{{key}}}", str(value))
        print(f"Executing SQL Query:\n{query}")
        return spark.sql(query)
    
    @staticmethod
    def execute_multi_query(spark, queries):
        """Execute multiple SQL statements"""
        results = []
        for query in queries:
            if query.strip():
                results.append(spark.sql(query))
        return results
    
    @staticmethod
    def execute_with_cte(spark, ctes, main_query):
        """Execute query with Common Table Expressions (CTEs)"""
        full_query = "WITH "
        cte_strings = []
        for cte_name, cte_query in ctes.items():
            cte_strings.append(f"{cte_name} AS ({cte_query})")
        full_query += ", ".join(cte_strings)
        full_query += f" {main_query}"
        return spark.sql(full_query)
