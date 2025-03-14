"""
You can create a shallow copy of an existing Delta table at a specific version using the shallow clone command.

Any changes made to shallow clones affect only the clones themselves and not the source table, as long as they don’t
touch the source data Parquet files.

The metadata that is cloned includes: schema, partitioning information, invariants, nullability. For shallow clones,
stream metadata is not cloned.

CREATE TABLE delta.`/data/target/` SHALLOW CLONE delta.`/data/source/` -- Create a shallow clone of /data/source at
/data/target

CREATE OR REPLACE TABLE db.target_table SHALLOW CLONE db.source_table -- Replace the target. target needs to be emptied

CREATE TABLE IF NOT EXISTS delta.`/data/target/` SHALLOW CLONE db.source_table -- No-op if the target table exists

CREATE TABLE db.target_table SHALLOW CLONE delta.`/data/source`

CREATE TABLE db.target_table SHALLOW CLONE delta.`/data/source` VERSION AS OF version

CREATE TABLE db.target_table SHALLOW CLONE delta.`/data/source` TIMESTAMP AS OF timestamp_expression -- timestamp can
be like “2019-01-01” or like date_sub(current_date(), 1)

Clone metrics

CLONE reports the following metrics as a single row DataFrame once the operation is complete:

    source_table_size: Size of the source table that’s being cloned in bytes.

    source_num_of_files: The number of files in the source table.

Clone Parquet or Iceberg table to Delta

CREATE OR REPLACE TABLE <target_table_name> SHALLOW CLONE parquet.`/path/to/data`;

CREATE OR REPLACE TABLE <target_table_name> SHALLOW CLONE iceberg.`/path/to/data`;

"""
if __name__ == '__main__':
    print('Hello World!')
