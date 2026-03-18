class PartitionOptimizer:
    """Optimize DataFrame partitioning"""
    
    @staticmethod
    def repartition_for_optimization(df, num_partitions=None, partition_cols=None):
        """Repartition DataFrame for optimization"""
        if partition_cols:
            return df.repartition(num_partitions or df.rdd.getNumPartitions(), 
                                  *partition_cols)
        return df.repartition(num_partitions or df.rdd.getNumPartitions())
    
    @staticmethod
    def coalesce_partitions(df, num_partitions):
        """Coalesce partitions (reduce number of partitions)"""
        return df.coalesce(num_partitions)
    
    @staticmethod
    def optimize_partition_size(df, target_size_mb=128):
        """Optimize partition count based on target size"""
        # Estimate DataFrame size (simplified)
        estimated_size_mb = df.count() * len(df.columns) * 8 / (1024 * 1024)
        optimal_partitions = max(1, int(estimated_size_mb / target_size_mb))
        return df.repartition(optimal_partitions)
    
    @staticmethod
    def bucket_by(df, num_buckets, bucket_cols, sort_cols=None):
        """Bucket DataFrame for optimization"""
        writer = df.write.bucketBy(num_buckets, *bucket_cols)
        if sort_cols:
            writer = writer.sortBy(*sort_cols)
        return writer
