name := "spark-resource-optimization"
version := "1.0"
ThisBuild / versionScheme := Some("early-semver")
scalaVersion := "2.11.12"
scalacOptions ++= Seq("-deprecation", "-unchecked")

val sparkVersion = "2.4.0-cdh6.3.3"
val scalaTestVersion = "3.2.10"

resolvers in Global += Resolver.mavenLocal

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots",
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  Resolver.sonatypeRepo("public")
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)