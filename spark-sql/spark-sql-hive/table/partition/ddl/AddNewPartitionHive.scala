package com.mahfooz.spark.sqlhive.table.partition.ddl

object AddNewPartitionHive {

  def main(args: Array[String]): Unit = {
    "ALTER TABLE zipcodes ADD PARTITION (state='CA') LOCATION '/user/data/zipcodes_ca'"
  }

}
