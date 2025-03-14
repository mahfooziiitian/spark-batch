ThisBuild / organization := "com.mahfooz.spark"
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.17"

name := "spark-caching"

resolvers in Global += Resolver.mavenLocal

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots",
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"
)

val cachingHdfs = project in file("spark-caching-hdfs")
val sparkMemory=project in file("spark-memory-caching")
val sparkDisk=project in file("spark-disk-caching")

