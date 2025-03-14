name := "spark-optimization"
version := "1.0"
scalaVersion := "2.11.12"
scalacOptions ++= Seq("-deprecation", "-unchecked")
libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaVersion.value

def getProject(name:String) : Project = getProject(name,name)
def getProject(name:String,path:String) : Project = Project(name,file(path))

lazy val caching=getProject("spark-caching")
lazy val storage=getProject("spark-persistence-storage")
lazy val repartitionCoalesce=getProject("spark-repartition-coalesce")
lazy val catalyst=getProject("catalyst-optimizer")
lazy val sparkSPartition=getProject("spark-partition")

lazy val sparkQueryOptimization = getProject("spark-query-optimization")

lazy val dataframeDatasetRdd = getProject("df-ds-over-rdd")

lazy val tungstenOptimizer = getProject("tungsten-optimizer")

lazy val sparkBucketing = getProject("spark-bucketting")
lazy val sparkCheckpoint = getProject("spark-checkpoint")

val sparkResourceOptimization =  getProject("spark-resource-optimization")
lazy val sparkDiskOptimization = getProject("spark-diskio-optimization")
lazy val sparkRuleExecutor = getProject("spark-rule-executor")
lazy val sparkSkewData = getProject("spark-skew-data")
lazy val sparkShuffling = getProject("spark-shuffling")
lazy val sparkStatsOptimization = getProject("spark-stats-optimization")
lazy val sparkStatsCompacting = getProject("spark-compacting")
lazy val sparkStatsCompression = getProject("spark-compression")
lazy val sparkPlanOptimization = getProject("spark-plan-optimization")



