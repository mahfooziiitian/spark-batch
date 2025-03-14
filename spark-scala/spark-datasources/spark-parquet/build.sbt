name := "spark-parquet"

ThisBuild / organization := "com.mahfooz.spark"
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.11.12"
Global / resolvers += Resolver.mavenLocal

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots",
  Resolver.sonatypeRepo("public")
)

val sparkVersion = "2.4.0-cdh6.3.3"
val sparkXmlVersion = "0.5.0"
val scalaTestVersion = "3.2.10"
val sparkUtil = "1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.spark.sql" %% "spark-sql-util" % sparkUtil,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)
