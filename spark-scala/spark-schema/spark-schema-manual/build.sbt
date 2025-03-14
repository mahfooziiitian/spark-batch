name := "spark-schema-manual"

ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / organization := "com.mahfooz.spark.schema"

val sparkVersion = "3.3.2"
val sparkHiveVersion = "3.3.2"
val sparkUtilVersion = "1.0"

resolvers in Global += Resolver.mavenLocal
resolvers in Global += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots",
  Resolver.sonatypeRepo("public")
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest-funsuite" % "3.2.15" % Test
)
