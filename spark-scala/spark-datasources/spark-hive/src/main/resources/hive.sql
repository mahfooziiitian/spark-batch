-- Creating partition table
CREATE TABLE hive_partitioned_table
(id BIGINT, name STRING)
COMMENT 'Hive Partitioned Parquet Table and Partition Pruning'
PARTITIONED BY (city STRING COMMENT 'City')
STORED AS PARQUET;

-- static partition
INSERT INTO hive_partitioned_table
PARTITION (city="Warsaw")
VALUES (0, 'Jacek');

INSERT INTO hive_partitioned_table
PARTITION (city="Paris")
VALUES (1, 'Agata');



