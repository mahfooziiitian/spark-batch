name := "spark-xml"
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.12"
ThisBuild / organization := "com.spark.sql"

val sparkVersion = "3.3.1"
val sparkUtilVersion = "1.0"
val mysqlVersion = "8.0.19"
val sparkXmlVersion = "0.16.0"

scalacOptions += "-target:jvm-1.8"
resolvers in Global += Resolver.mavenLocal
resolvers in Global += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots"
)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe" % "config" % "1.4.2",
  "mysql" % "mysql-connector-java" % mysqlVersion,
  "com.databricks" %% "spark-xml" % sparkXmlVersion,
  "com.thoughtworks.xstream" % "xstream" % "1.4.20"
)