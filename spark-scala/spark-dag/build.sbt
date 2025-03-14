name := "spark-dag"

ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / organization := "com.mahfooz.spark"

resolvers in Global += Resolver.mavenLocal

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots",
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.2"
)