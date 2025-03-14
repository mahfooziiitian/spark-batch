organization := "com.mahfooz.spark"
name := "spark-dataframe-action"
version := "1.0"

ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.spark.sql"

val sparkVersion = "3.4.0"
val scalaTestVersion = "3.2.16"

Global /resolvers += Resolver.mavenLocal

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)
