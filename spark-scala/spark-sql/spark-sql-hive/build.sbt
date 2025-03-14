
name := "spark-sql-hive"

ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.spark.sql"

val sparkVersion = "3.4.0"
val sparkHiveVersion = "3.4.0"
val jacksonCoreVersion = "2.12.2"
val oracleJdbcVersion = "12.2.0.1"
val sparkUtilVersion = "1.0"

resolvers in Global += Resolver.mavenLocal
resolvers in Global += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots"
)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  //"com.fasterxml.jackson.core" % "jackson-core" % jacksonCoreVersion,
  "com.oracle.database.jdbc" % "ojdbc8" % oracleJdbcVersion,
  //"com.spark.sql" %% "spark-sql-util" % sparkUtilVersion
)

