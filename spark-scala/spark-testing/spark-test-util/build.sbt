name := "spark-testing-util"
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
  "io.cucumber" % "cucumber-core" % "6.10.0",
  "io.cucumber" % "cucumber-java" % "6.10.0",
  "org.mockito" % "mockito-core" % "3.5.13",
  "org.mockito" % "mockito-inline" % "3.5.13",
  "org.scalatest" %% "scalatest" % "3.2.0" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "io.cucumber" % "cucumber-junit" % "6.10.0" % "test",
  "io.cucumber" % "cucumber-guice" % "6.10.0" % "test"
)
