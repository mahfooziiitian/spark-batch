"""
Inside a given Spark application (SparkContext instance), multiple parallel jobs can run simultaneously if they were submitted from separate threads.
By "job", in this section, we mean a Spark action (e.g. save, collect) and any tasks that need to run to evaluate that action.
Spark’s scheduler is fully thread-safe and supports this use case to enable applications that serve multiple requests (e.g. queries for multiple users).
By default, Spark’s scheduler runs jobs in FIFO fashion.
Each job is divided into “stages” (e.g. map and reduce phases), and the first job gets priority on all available resources while its stages have tasks to launch,
then the second job gets priority, etc.
If the jobs at the head of the queue don’t need to use the whole cluster, later jobs can start to run right away, but if the jobs at the head of the queue are large,
then later jobs may be delayed significantly.
Starting in Spark 0.8, it is also possible to configure fair sharing between jobs. Under fair sharing, Spark assigns tasks between jobs in a "round robin" fashion,
so that all jobs get a roughly equal share of cluster resources.
This means that short jobs submitted while a long job is running can start receiving resources right away and still get good response times, without waiting for the long
job to finish. This mode is best for multi-user settings.

To enable the fair scheduler, simply set the spark.scheduler.mode property to FAIR when configuring a SparkContext:

"""
import os
import sys

from pyspark import SparkConf, SparkContext

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark_conf = SparkConf().setMaster("local[*]").setAppName("scheduling_within_application")
    spark_conf.set("spark.scheduler.mode", "FAIR")
    spark_conf.set("spark.scheduler.allocation.file", "fairscheduler.xml")
    sc = SparkContext(conf=spark_conf)

    # Fair Scheduler Pools
    sc.setLocalProperty("spark.scheduler.pool", "production")
