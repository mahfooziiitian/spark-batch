name := "spark-flume"

ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.10.5"

lazy val app=project in file("spark-app")
lazy val sink=project in file("spark-sink")
lazy val source=project in file("spark-source")

