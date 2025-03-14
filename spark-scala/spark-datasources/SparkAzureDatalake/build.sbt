name := "SparkAzureDatalake"
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.13.8"
ThisBuild / organization := "com.spark.sql"

val sparkVersion = "3.3.2"
val sparkUtilVersion = "1.0"
val hadoopAzureVersion = "3.3.4"

javacOptions ++= Seq("-source", "8", "-target", "8")

resolvers in Global += Resolver.mavenLocal

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots"
)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scala-sbt" %% "compiler-bridge" % "1.8.0" withSources(),
  "com.typesafe" % "config" % "1.4.2",
  "org.apache.hadoop" % "hadoop-azure" % hadoopAzureVersion
)