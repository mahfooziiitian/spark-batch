name := "spark-kafka-abris"
version := "1.0"
scalaVersion := "2.11.12"

resolvers in Global += Resolver.mavenLocal
resolvers in Global += "Cloudera" at  "https://repository.cloudera.com/artifactory/cloudera-repos/"
resolvers in Global += "Confluent" at  "https://packages.confluent.io/maven/"

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots",
  Resolver.sonatypeRepo("public")
)

libraryDependencies ++= Seq(
  "za.co.absa" % "abris_2.11" % "3.1.1" exclude("com.fasterxml.jackson.core","jackson-databind"),
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
)
