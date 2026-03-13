name := "spark-xml"
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.20"
ThisBuild / organization := "com.spark.sql"

val sparkVersion = "3.3.1"
val sparkUtilVersion = "1.0"
val mysqlVersion = "8.0.20"
val sparkXmlVersion = "0.18.0"

scalacOptions += "-target:jvm-11"
resolvers in Global += Resolver.mavenLocal

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots"
)
resolvers += Resolver.sonatypeCentralSnapshots

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe" % "config" % "1.4.4",
  "mysql" % "mysql-connector-java" % mysqlVersion,
  "com.databricks" %% "spark-xml" % sparkXmlVersion,
  "com.thoughtworks.xstream" % "xstream" % "1.4.20",
  "com.lihaoyi" %% "os-lib" % "0.11.4"
)