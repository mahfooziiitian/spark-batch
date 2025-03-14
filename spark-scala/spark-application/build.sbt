ThisBuild / organization := "com.mahfooz.spark"
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.18"
name := "spark-application"
Global / resolvers += Resolver.mavenLocal

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots",
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  Resolver.sonatypeRepo("public")
)

val sparkVersion = "3.3.2"
val scalaTestVersion = "3.2.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)