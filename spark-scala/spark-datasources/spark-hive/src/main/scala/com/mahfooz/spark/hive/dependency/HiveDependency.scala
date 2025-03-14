/*

Spark SQL also supports reading and writing data stored in Apache Hive.
However, since Hive has a large number of dependencies, these dependencies are not included in the default Spark distribution.
If Hive dependencies can be found on the classpath, Spark will load them automatically.
Note that these Hive dependencies must also be present on all of the worker nodes, as they will need access to the Hive serialization and
deserialization libraries (SerDes) in order to access data stored in Hive.

  export CLASSPATH=$CLASSPATH:<hive-jar-path>

 */
package com.mahfooz.spark.hive.dependency

object HiveDependency {

}
