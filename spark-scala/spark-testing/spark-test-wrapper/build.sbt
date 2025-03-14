name := "spark-testing-wrapper"
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
  "org.apache.spark" %% "spark-sql" % "2.4.0-cdh6.3.3",
  "com.github.mrpowers" %% "spark-fast-tests" % "0.23.0" % "test",
  "org.scalatest" %% "scalatest" % "3.2.0" % "test"
)
