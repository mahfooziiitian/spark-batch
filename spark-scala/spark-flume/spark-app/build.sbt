name := "spark-app"

resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
resolvers += Resolver.mavenLocal

libraryDependencies ++=Seq(
  "org.apache.spark" %% "spark-streaming-flume" % "1.6.0-cdh5.16.1",
  "org.apache.spark" %% "spark-streaming" % "1.6.0-cdh5.16.1",
  "org.apache.spark" %% "spark-core" % "1.6.0-cdh5.16.1"
)
