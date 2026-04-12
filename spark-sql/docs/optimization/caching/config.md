# :material-lightning-bolt: Caching config

### :material-sitemap: Overview

```mermaid
graph LR
    A[Config Setting] --> B[CacheManager]
    B --> C[InMemoryRelation]
    C --> D[Columnar Store]
```

## 🔧 Common SQL Caching Configurations

Config                                       | Description                                                          | Default
---------------------------------------------|----------------------------------------------------------------------|------------------
spark.sql.inMemoryColumnarStorage.compressed | Whether to compress cached data                                      | true
spark.sql.inMemoryColumnarStorage.batchSize  | Batch size for columnar cache (in rows)                              | 10000
spark.sql.cache.serializer                   | Serializer for cached tables (default is Kryo/Java based on context) | -
spark.sql.autoBroadcastJoinThreshold         | Can interact with caching for joins                                  | 10MB
spark.sql.cache.level                        | Set default cache storage level (since Spark 3.3+)                   | MEMORY_AND_DISK
spark.sql.defaultSizeInBytes                 | Default size used when no stats are available                        | 1GB

## ✅ How to Set Caching Configs in SQL

You can set these directly in a SQL notebook or script:

```sql
-- Enable compression for cached tables
SET spark.sql.inMemoryColumnarStorage.compressed = true;

-- Change batch size for caching
SET spark.sql.inMemoryColumnarStorage.batchSize = 5000;

-- Set default cache level (Spark 3.3+)
SET spark.sql.cache.level = MEMORY_ONLY;
```

## How Spark SQL Caching Works

When you run:

```sql
CACHE TABLE my_view;
```

1. Spark stores the data in columnar format in memory.
2. It only caches once a query triggers materialization (e.g., SELECT COUNT(*)).
3. Compression saves memory at a slight CPU cost.
4. Storage level controls whether Spark spills to disk if memory is insufficient.

## 🔍 Check Cache Status
You can check what’s cached:

```sql
-- List cached tables
SHOW TABLES;
```
