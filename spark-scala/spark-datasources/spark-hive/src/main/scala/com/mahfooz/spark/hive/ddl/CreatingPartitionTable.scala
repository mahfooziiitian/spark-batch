package com.mahfooz.spark.hive.ddl

import com.mahfooz.spark.hive.common.SparkHiveCommon

object CreatingPartitionTable {

  def main(args: Array[String]): Unit = {

    val spark=SparkHiveCommon.createSparkDynamicSession("CreatingPartitionTable")

    // CREATE HIVE TABLE (with one row)
    spark.sql("""
             CREATE TABLE IF NOT EXISTS hive_df (col1 INT, col2 STRING, partition_bin INT)
             USING HIVE OPTIONS(fileFormat 'PARQUET')
             PARTITIONED BY (partition_bin)
             """)
    spark.sql("""
             INSERT INTO hive_df PARTITION (partition_bin = 0)
             VALUES (0, 'init_record')
             """)

    //CREATE NON HIVE TABLE (with one row)
    spark.sql("""
             CREATE TABLE IF NOT EXISTS non_hive_df (col1 INT, col2 STRING, partition_bin INT)
             USING PARQUET
             PARTITIONED BY (partition_bin)
             """)

    spark.sql("""
             INSERT INTO non_hive_df PARTITION (partition_bin = 0)
             VALUES (0, 'init_record')
             """)

    // ATTEMPT DYNAMIC OVERWRITE WITH EACH TABLE
    spark.sql("""
             INSERT OVERWRITE TABLE hive_df PARTITION (partition_bin)
             VALUES (0, 'new_record', 1)
             """)
    spark.sql("""
             INSERT OVERWRITE TABLE non_hive_df PARTITION (partition_bin)
             VALUES (0, 'new_record', 1)
             """)

    //# 2 row dynamic overwrite
    spark.sql("SELECT * FROM hive_df").show()

    //# 1 row full table overwrite
    spark.sql("SELECT * FROM non_hive_df").show()

    spark.stop()
  }
}
