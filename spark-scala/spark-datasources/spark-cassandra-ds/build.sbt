name := "SparkCassandraDs"


ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.11.12"
ThisBuild / organization := "com.mahfooz.spark"

resolvers in Global += Resolver.mavenLocal

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots",
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.5.2",
  "com.github.jnr" % "jnr-posix" % "3.1.16"
)