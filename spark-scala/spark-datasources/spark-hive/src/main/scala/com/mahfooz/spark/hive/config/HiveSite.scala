/*


1)  Move hive-site.xml from $HIVE_HOME/conf/hive-site.xml to $SPARK_HOME/conf/hive-site.xml.
2)  Extract all the dependencies for required Spark components
3)  Run the Hive metastore process so that when Spark SQL runs, it can connect to metastore uris
    and take from it the hive-site.xml file mentioned in the first step.
4)

Configuration of Hive is done by placing your hive-site.xml, core-site.xml (for security configuration), and hdfs-site.xml
(for HDFS configuration) file in conf/.

When not configured by the hive-site.xml, the context automatically creates metastore_db in the current directory and creates a directory
configured by spark.sql.warehouse.dir, which defaults to the directory spark-warehouse in the current directory that the Spark
application is started.

  hive.metastore.warehouse.dir
     hive.metastore.warehouse.dir property in hive-site.xml is deprecated since Spark 2.0.0.

  spark.sql.warehouse.dir
      spark.sql.warehouse.dir to specify the default location of database in warehouse.

 */
package com.mahfooz.spark.hive.config

class HiveSite {

}
