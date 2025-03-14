Since the data is stored in columns, it can be highly compressed (compression algorithms work better with low information entropy data, which is usually contained in columns) and separated. Software developers say this storage system is suitable for solving problems with Big Data.

Parquet files, unlike CSV and JSON, in apache spark are binary files which contain metadata about their contents. Spark can therefore simply rely on metadata to decide column names, compression or encoding, data types and even some basic statistical characteristics without reading or parsing the contents of the file(s). At the end of the file, column metadata is stored for a Parquet file, which allows quick, single pass writing.

Parquet is adapted to Write Once Read Most (WORM) model. It writes slowly but reads extremely quickly, particularly when only a subset of columns is accessed. Parquet is a safe choice when reading parts of the data for heavy workloads. In cases where you need to deal with entire rows of data, a format such as CSV or AVRO should be used.

# Choosing file format

Choosing an appropriate file format can have some significant benefits:

1. Faster read times
2. Faster write times
3. Splittable files
4. Schema evolution support
5. Advanced compression support


# Advantages of parquet data storage

1. Parquet in apache spark is a columnar format. Only the correct columns are retrieved or read, and this reduces the I or O disk.
2. The concept is called **projection pushdown**.
3. Data flows with the system and data is self-describing.
4. Just parquet files which means that they can be quickly accessed, transferred, backed up and replicated.
5. Built-in support in Spark makes file access and access simple.


# Disadvantages

The column-based nature makes you think about the scheme and data types. Parquet doesn’t always have built-in support in software other than Spark; it doesn’t support data alteration parquet files are immutable and scheme evolution. Of course, if you change the schema over time, Spark knows how to merge it you must define a special option when reading, but you can only change something in an existing file by overwriting it.

# AVRO vs PARQUET
1. AVRO is a row-based storage format, whereas PARQUET is a columnar-based storage format. 
2. PARQUET is much better for analytical querying, i.e., reads and querying are much more efficient than writing. 
3. Writing operations in AVRO are better than in PARQUET. 
4. AVRO is much matured than PARQUET when it comes to schema evolution. PARQUET only supports schema append, whereas AVRO supports a much-featured schema evolution, i.e., adding or modifying columns. 
5. PARQUET is ideal for querying a subset of columns in a multi-column table. AVRO is ideal in the case of ETL operations, where we need to query all the columns.

# ORC vs. PARQUET

1. PARQUET is more capable of storing nested data. 
2. ORC is more capable of Predicate Pushdown. 
3. ORC supports ACID properties. 
4. ORC is more compression efficient.

