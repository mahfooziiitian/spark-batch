|Property Name	|Default|	Meaning|
|----|----|----|
|spark.eventLog.compress	|false	|Whether to compress logged events, if spark.eventLog.enabled is true. Compression will use spark.io.compression.codec.|
spark.eventLog.dir	file:///tmp/spark-events	Base directory in which Spark events are logged, if spark.eventLog.enabled is true. Within this base directory, Spark creates a sub-directory for each application, and logs the events specific to the application in this directory. Users may want to set this to a unified location like an HDFS directory so history files can be read by the history server.
spark.eventLog.enabled	false	Whether to log Spark events, useful for reconstructing the Web UI after the application has finished.
spark.ui.enabled	true	Whether to run the web UI for the Spark application.
spark.ui.killEnabled	true	Allows jobs and stages to be killed from the web UI.
spark.ui.port	4040	Port for your application's dashboard, which shows memory and workload data.
spark.ui.retainedJobs	1000	How many jobs the Spark UI and status APIs remember before garbage collecting. This is a target maximum, and fewer elements may be retained in some circumstances.
spark.ui.retainedStages	1000	How many stages the Spark UI and status APIs remember before garbage collecting. This is a target maximum, and fewer elements may be retained in some circumstances.
spark.ui.retainedTasks	100000	How many tasks the Spark UI and status APIs remember before garbage collecting. This is a target maximum, and fewer elements may be retained in some circumstances.
spark.ui.reverseProxy	false	Enable running Spark Master as reverse proxy for worker and application UIs. In this mode, Spark master will reverse proxy the worker and application UIs to enable access without requiring direct access to their hosts. Use it with caution, as worker and application UI will not be accessible directly, you will only be able to access them through spark master/proxy public URL. This setting affects all the workers and application UIs running in the cluster and must be set on all the workers, drivers and masters.
spark.ui.reverseProxyUrl		This is the URL where your proxy is running. This URL is for proxy which is running in front of Spark Master. This is useful when running proxy for authentication e.g. OAuth proxy. Make sure this is a complete URL including scheme (http/https) and port to reach your proxy.
spark.ui.showConsoleProgress	true	Show the progress bar in the console. The progress bar shows the progress of stages that run for longer than 500ms. If multiple stages run at the same time, multiple progress bars will be displayed on the same line.
spark.worker.ui.retainedExecutors	1000	How many finished executors the Spark UI and status APIs remember before garbage collecting.
spark.worker.ui.retainedDrivers	1000	How many finished drivers the Spark UI and status APIs remember before garbage collecting.
spark.sql.ui.retainedExecutions	1000	How many finished executions the Spark UI and status APIs remember before garbage collecting.
spark.streaming.ui.retainedBatches	1000	How many finished batches the Spark UI and status APIs remember before garbage collecting.
spark.ui.retainedDeadExecutors	100	How many dead executors the Spark UI and status APIs remember before garbage collecting.