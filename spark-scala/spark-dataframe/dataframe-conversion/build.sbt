organization := "com.mahfooz.spark"
version := "1.0"
name := "dataframe-conversion"

ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.spark.sql"

val sparkVersion = "3.4.0"
val scalaTestVersion = "3.2.16"
val sparkUtilVersion = "1.0"

Global /resolvers += Resolver.mavenLocal

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)
