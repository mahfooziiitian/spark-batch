name := "spark-rdd-partition"
version := "1.0"
scalaVersion := "2.12.17"

Global /resolvers += Resolver.mavenLocal

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots",
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.4.1",
  "org.apache.spark" %% "spark-streaming" % "3.4.1",
  "com.mysql" % "mysql-connector-j" % "8.2.0",
  "org.scalatest" %% "scalatest-funsuite" % "3.2.15" % Test
)
