# Hive Metastore

The Hive Metastore is the most widely used and traditional metadata store for Spark and other big data tools. It manages metadata for tables, partitions, and schemas, storing this information in a relational database such as MySQL, PostgreSQL, or Derby.

## Key Features

- **External and Managed Tables:** Supports both external tables (data stored outside Hive) and managed tables (data managed by Hive).
- **Partitioning:** Enables efficient querying and management of large datasets by partitioning tables.
- **Schema Evolution:** Allows changes to table schemas over time without losing existing data.
- **Integration:** Seamlessly integrates with Apache Hive, Apache Iceberg, and Delta Lake for unified metadata management.

## Access

- Accessed via a Thrift URI:  
    `thrift://<host>:<port>`

## Use Cases

- Centralized metadata management for Spark SQL, Hive, and other analytics engines.
- Enables interoperability between different data processing frameworks.

For more details, refer to the [Hive Metastore documentation](https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+Administration).
