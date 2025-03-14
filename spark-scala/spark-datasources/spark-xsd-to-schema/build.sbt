ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-xsd-to-schema",
    organization := "com.everest.xml"
  )

val sparkVersion = "3.3.1"
val sparkXmlVersion = "0.16.0"

scalacOptions += "-target:jvm-1.8"
resolvers in Global += Resolver.mavenLocal

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots"
)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.databricks" %% "spark-xml" % sparkXmlVersion
)