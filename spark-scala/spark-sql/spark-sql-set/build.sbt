
name := "spark-sql-set"

version := "1.0"
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.spark.sql"

val sparkVersion = "3.4.0"
val sparkUtilVersion = "1.0"

resolvers in Global += Resolver.mavenLocal
resolvers in Global += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
//resolvers in Global += Resolver.sonatypeOssRepos("snapshots")
resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots"
)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  //"com.spark.sql" %% "spark-sql-util" % sparkUtilVersion
)
