name := "spark-sql-dependency"

ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.spark.sql"

val sparkVersion = "3.4.0"
val sparkUtilVersion = "1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.oracle.database.jdbc" % "ojdbc8" % "12.2.0.1"
)
