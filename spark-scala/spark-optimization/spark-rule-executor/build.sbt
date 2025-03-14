name := "spark-rule-executor"
version := "1.0"
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.11.12"
ThisBuild / organization := "com.spark.sql"

val sparkVersion = "2.4.0-cdh6.3.3"
val sparkUtilVersion = "1.0"

scalacOptions += "-target:jvm-1.8"
resolvers in Global += Resolver.mavenLocal
resolvers in Global += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots",
  "pentaho" at "https://public.nexus.pentaho.org/content/groups/omni/",
  Resolver.sonatypeRepo("public")
)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.spark.sql" %% "spark-sql-util" % sparkUtilVersion
)