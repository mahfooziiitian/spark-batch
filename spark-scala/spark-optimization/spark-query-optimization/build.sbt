name := "spark-query-optimization"
version := "1.0"
ThisBuild / versionScheme := Some("early-semver")
scalaVersion := "2.11.12"
scalacOptions ++= Seq("-deprecation", "-unchecked")
libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaVersion.value


