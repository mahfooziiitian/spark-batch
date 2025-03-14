/*

native implementation supports a vectorized ORC reader and has been the default ORC implementation since Spark 2.3.

The vectorized reader is used for the native ORC tables (e.g., the ones created using the clause USING ORC)
when spark.sql.orc.impl is set to native and spark.sql.orc.enableVectorizedReader is set to true.

For nested data types (array, map and struct), vectorized reader is disabled by default.

Set spark.sql.orc.enableNestedColumnVectorizedReader to true to enable vectorized reader for these types.

For the Hive ORC serde tables (e.g., the ones created using the clause USING HIVE OPTIONS (fileFormat 'ORC')),
the vectorized reader is used when spark.sql.hive.convertMetastoreOrc is also set to true, and is turned on by default.
 */
package com.mahfooz.spark.orc.vectorized

object VectorizedReaderHive {

  def main(args: Array[String]): Unit = {

  }

}
