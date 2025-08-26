# Introduction

In Spark SQL, caching is a way to persist query results or tables in memory (and optionally on disk) to speed up subsequent queries that reuse the same data. Caching avoids recomputing or rereading data from slower sources like disk or remote storage.

## 🔥 Spark SQL Caching Overview

1. Cache: Store data in memory for faster access.
2. Persist: Cache with different storage levels (memory-only, memory+disk, etc.).
3. Spark SQL lets you cache tables, views, or query results.
4. Useful when the same data is accessed multiple times in your session or notebook.

## When to Cache?

1. When the same table/view is queried multiple times.
2. When iterative algorithms or dashboards query the same data repeatedly.
3. To speed up joins or filters on cached data.
