from pyspark.sql import functions as F

class ViewManager:
    """Manage temporary and global views"""
    
    @staticmethod
    def create_temp_view(df, view_name):
        """Create temporary view"""
        df.createOrReplaceTempView(view_name)
        return view_name
    
    @staticmethod
    def create_global_view(df, view_name):
        """Create global temporary view"""
        df.createOrReplaceGlobalTempView(view_name)
        return f"global_temp.{view_name}"
    
    @staticmethod
    def drop_view(spark, view_name, global_view=False):
        """Drop a view"""
        if global_view:
            spark.catalog.dropGlobalTempView(view_name.replace("global_temp.", ""))
        else:
            spark.catalog.dropTempView(view_name)
    
    @staticmethod
    def list_views(spark, global_only=False):
        """List all views"""
        if global_only:
            return spark.catalog.listGlobalTempViews()
        return spark.catalog.listTables()
