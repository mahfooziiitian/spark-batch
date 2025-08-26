# Cache Manager

In Spark SQL, the Cache Manager is the internal component responsible for handling the caching and un-caching of structured data like DataFrames, Datasets, and temporary views.

It acts as a layer that manages:

1. What to cache
2. When to cache
3. How to evict or unpersist
4. Memory usage tracking

`org.apache.spark.sql.CacheManager` is Spark's internal class that:

1. Tracks cached tables and DataFrames
2. Interfaces with the In-Memory TableScan
3. Uses in-memory columnar storage
4. Ensures that cached data remains consistent with Spark's query plan

## 🧱 Components Behind the Scenes

Component                 | Role
--------------------------|--------------------------------------------------
CacheManager              | Registers cached plans and tracks usage
InMemoryRelation          | Logical plan wrapper for cached data
InMemoryTableScanExec     | Physical operator to read from cache
InMemoryColumnarTableScan | Used for columnar storage access
StorageLevel              | Defines how/where the cache is stored (RAM, Disk)

## Step-by-step

CACHE TABLE sales_data;

1. → Spark SQL Parser parses statement
2. → Logical plan is generated
3. → CacheManager stores plan in a registry
4. → On first action (e.g., COUNT), Spark materializes the data
5. → Data is stored in memory in columnar format
6. → Later queries use InMemoryTableScanExec instead of recomputing
