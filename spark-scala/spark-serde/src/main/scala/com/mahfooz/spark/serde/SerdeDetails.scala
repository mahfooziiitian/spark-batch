/*
It provides two serialization libraries:

Java serialization:

  By default, Spark serializes objects using Java’s ObjectOutputStream framework, and can work with any class you create that
  implements java.io.Serializable.
  You can also control the performance of your serialization more closely by extending java.io.Externalizable.
  Java serialization is flexible but often quite slow, and leads to large serialized formats for many classes.

Kryo serialization:
  Spark can also use the Kryo library (version 4) to serialize objects more quickly.
  Kryo is significantly faster and more compact than Java serialization (often as much as 10x), but does not support all Serializable
  types and requires you to register the classes you’ll use in the program in advance for best performance.

You can switch to using Kryo by initializing your job with a SparkConf and calling
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


val conf = new SparkConf().setMaster(...).setAppName(...)
conf.registerKryoClasses(Array(classOf[MyClass1], classOf[MyClass2]))
val sc = new SparkContext(conf)


 */
package com.mahfooz.spark.serde

object SerdeDetails {

}
