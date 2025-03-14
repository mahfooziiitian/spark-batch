The Dataset API, released as an API preview in Spark 1.6, aims to provide the best of both worlds: the familiar object-oriented programming style and compile-time type-safety of the RDD API but with the performance benefits of the Catalyst query optimizer.

Datasets also use the same efficient off-heap storage mechanism as the DataFrame API.


When it comes to serializing data, the Dataset API has the concept of encoders which translate between 
JVM representations (objects) and Spark’s internal binary format.

Spark has very advanced built-in encoders that can generate byte code to interact with 
off-heap data. 

They also provide on-demand access to individual attributes without having to 
de-serialize an entire object. 

Spark does not yet offer an API for implementing custom encoders, but that is planned for 
a future release.

Additionally, the Dataset API is designed to work equally well with both Java and Scala.

When working with Java objects, they must be fully bean-compliant.

In writing the examples to accompany this article, errors occurred errors when trying to create a Dataset in Java from a list of Java objects that were not fully bean-compliant.

The encoder maps the domain-specific type T to Spark’s internal type system.

For example, given a class Person with two fields, name (string) and age (int), an encoder directs Spark to generate code at runtime to serialize the Person object into a binary structure.

When using DataFrames or the "standard" Structured APIs, this binary structure will be a Row.

When we want to create our own domain-specific objects, we specify a case class in Scala or a JavaBean in Java.

Spark will allow us to manipulate this object (in place of a Row) in a distributed manner.

When you use the Dataset API, for every row it touches, this domain specifies type, Spark
converts the Spark Row format to the object you specified (a case class or Java class).
This conversion slows down your operations but can provide more flexibility.

