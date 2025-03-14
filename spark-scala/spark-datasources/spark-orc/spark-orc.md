Apache Spark on HDP supports the Optimized Row Columnar (ORC) file format, a self-describing, type-aware, column-based file format that is one of the primary file formats supported in Apache Hive.

ORC reduces I/O overhead by accessing only the columns that are required for the current query.

It requires significantly fewer seek operations because all columns within a single group of row data (known as a stripe) are stored together on disk.

Spark ORC data source supports 

1. ACID transactions
2. Snapshot isolation
3. Built-in indexes
4. Complex data types (such as array, map, and struct)
5. Provides read and write access to ORC files. 
6. It leverages the Spark SQL Catalyst engine for common optimizations such as column pruning, predicate push-down, and partition pruning.
