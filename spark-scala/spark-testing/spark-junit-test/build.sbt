name := "spark-junit-test"
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
  //"org.scalatest" %% "scalatest" % "3.2.10" % Test,
  "org.scalatestplus" %% "junit-4-13" % "3.2.10.0" % Test
  //"junit" % "junit" % "4.13" % Test
)
