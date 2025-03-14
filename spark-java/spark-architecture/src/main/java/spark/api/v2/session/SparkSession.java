/*

SparkSession is now the new entry point of Spark that replaces the old SQLContext andHiveContext.

Note that the old SQLContext and HiveContext are kept for backward compatibility.

 A new catalog interface is accessible from SparkSession - existing API on databases and tables access such as listTables, 
 createExternalTable, dropTempView, cacheTable are moved here.

import org.apache.spark.sql.SparkSession

 */
package spark.api.v2.session;

public class SparkSession {

}
