name := "spark-elastic-search"
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.11"
ThisBuild / organization := "com.spark.sql"

val sparkVersion = "3.3.2"
val sparkUtilVersion = "1.0"
val elasticVersion = "8.6.2"

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
  "org.elasticsearch" %% "elasticsearch-spark-20" % "8.6.2",
  "com.typesafe" % "config" % "1.4.2"
)