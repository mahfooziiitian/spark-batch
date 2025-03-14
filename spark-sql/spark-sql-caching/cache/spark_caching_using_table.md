# spark sql caching

SQL CACHE TABLE is eager

The following gives "In-memory table `hundred`"

  CACHE TABLE hundred

## Configuration of in-memory caching

### spark.sql.inMemoryColumnarStorage.compressed

When set to true, Spark SQL will automatically select a compression codec for each column based on statistics of the data.

## spark.sql.inMemoryColumnarStorage.batchSize

Controls the size of batches for columnar caching.

Larger batch sizes can improve memory utilization and compression, but risk OOMs when caching data.
