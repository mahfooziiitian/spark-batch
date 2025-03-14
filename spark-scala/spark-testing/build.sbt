name := "spark-testing"
version := "1.0"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  Resolver.mavenLocal,
  Resolver.mavenCentral,
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  Resolver.sonatypeRepo("public")
)

libraryDependencies ++= Seq(
)

lazy val sparkTestUtil = project in file("spark-test-util")
lazy val sparkTestWrapper = project in file("spark-test-wrapper")
lazy val sparkTestingBase = project in file("spark-testing-base")
lazy val sparkScalaTest = project in file("spark-scalatest")
lazy val sparkJunitTest = project in file("spark-junit-test")
