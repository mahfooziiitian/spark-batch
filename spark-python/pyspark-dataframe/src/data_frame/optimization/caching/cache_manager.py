
class CacheManager:
    """Manage DataFrame caching and persistence"""
    
    @staticmethod
    def cache_dataframe(df, storage_level=None):
        """Cache DataFrame with specified storage level"""
        if storage_level:
            return df.persist(storage_level)
        return df.cache()
    
    @staticmethod
    def uncache_dataframe(df):
        """Uncache DataFrame"""
        df.unpersist()
    
    @staticmethod
    def check_cached(spark, df):
        """Check if DataFrame is cached"""
        catalog = spark.catalog
        table_name = f"df_{id(df)}"
        print(f"Catalog: {catalog}")
        print(f"table name: {table_name}")
        # This is a simplified check
        return df.is_cached
    
    @staticmethod
    def cache_table(spark, table_name, storage_level=None):
        """Cache a table by name"""
        if storage_level:
            spark.catalog.cacheTable(table_name, storage_level)
        else:
            spark.catalog.cacheTable(table_name)
    
    @staticmethod
    def clear_all_cache(spark):
        """Clear all cached data"""
        spark.catalog.clearCache()
