

| Environment Variable  | Meaning                                                                                                                                                                                  |
|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| JAVA_HOME             | Location where Java is installed (if it's not on your default PATH).                                                                                                                     |
| PYSPARK_PYTHON        | Python binary executable to use for PySpark in both driver and workers (default is python2.7 if available, otherwise python). Property spark.pyspark.python take precedence if it is set |
| PYSPARK_DRIVER_PYTHON | Python binary executable to use for PySpark in driver only (default is PYSPARK_PYTHON). Property spark.pyspark.driver.python take precedence if it is set                                |
| SPARKR_DRIVER_R       | R binary executable to use for SparkR shell (default is R). Property spark.r.shell.command take precedence if it is set                                                                  |
| SPARK_LOCAL_IP        | IP address of the machine to bind to.                                                                                                                                                    |
| SPARK_PUBLIC_DNS      | Hostname your Spark program will advertise to other machines.                                                                                                                            |

