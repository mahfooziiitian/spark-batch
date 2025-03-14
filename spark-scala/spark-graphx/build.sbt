
name := "spark-graphx"

ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.11.12"
ThisBuild / organization := "com.mahfooz.spark.graphx"

resolvers in Global += Resolver.mavenLocal
resolvers in Global += "Cloudera" at  "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots",
  Resolver.sonatypeRepo("public")
)

val graphxbasic=project in file("graphx-basic")
val graphxoperators=project in file("graph-operators")
