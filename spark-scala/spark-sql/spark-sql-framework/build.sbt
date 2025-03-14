name := "spark-sql-framework"

ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.spark.sql"

val sparkVersion = "3.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.oracle.database.jdbc" % "ojdbc8" % "12.2.0.1"
)
