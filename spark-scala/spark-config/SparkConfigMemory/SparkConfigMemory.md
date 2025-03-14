Property Name	Default	Meaning
spark.memory.fraction	0.6	Fraction of (heap space - 300MB) used for execution and storage. The lower this is, the more frequently spills and cached data eviction occur. The purpose of this config is to set aside memory for internal metadata, user data structures, and imprecise size estimation in the case of sparse, unusually large records. Leaving this at the default value is recommended. For more detail, including important information about correctly tuning JVM garbage collection when increasing this value, see this description.
spark.memory.storageFraction	0.5	Amount of storage memory immune to eviction, expressed as a fraction of the size of the region set aside by s​park.memory.fraction. The higher this is, the less working memory may be available to execution and tasks may spill to disk more often. Leaving this at the default value is recommended. For more detail, see this description.
spark.memory.offHeap.enabled	false	If true, Spark will attempt to use off-heap memory for certain operations. If off-heap memory use is enabled, then spark.memory.offHeap.size must be positive.
spark.memory.offHeap.size	0	The absolute amount of memory in bytes which can be used for off-heap allocation. This setting has no impact on heap memory usage, so if your executors' total memory consumption must fit within some hard limit then be sure to shrink your JVM heap size accordingly. This must be set to a positive value when spark.memory.offHeap.enabled=true.
spark.memory.useLegacyMode	false	​Whether to enable the legacy memory management mode used in Spark 1.5 and before. The legacy mode rigidly partitions the heap space into fixed-size regions, potentially leading to excessive spilling if the application was not tuned. The following deprecated memory fraction configurations are not read unless this is enabled: spark.shuffle.memoryFraction
spark.storage.memoryFraction
spark.storage.unrollFraction
spark.shuffle.memoryFraction	0.2	(deprecated) This is read only if spark.memory.useLegacyMode is enabled. Fraction of Java heap to use for aggregation and cogroups during shuffles. At any given time, the collective size of all in-memory maps used for shuffles is bounded by this limit, beyond which the contents will begin to spill to disk. If spills are often, consider increasing this value at the expense of spark.storage.memoryFraction.
spark.storage.memoryFraction	0.6	(deprecated) This is read only if spark.memory.useLegacyMode is enabled. Fraction of Java heap to use for Spark's memory cache. This should not be larger than the "old" generation of objects in the JVM, which by default is given 0.6 of the heap, but you can increase it if you configure your own old generation size.
spark.storage.unrollFraction	0.2	(deprecated) This is read only if spark.memory.useLegacyMode is enabled. Fraction of spark.storage.memoryFraction to use for unrolling blocks in memory. This is dynamically allocated by dropping existing blocks when there is not enough free storage space to unroll the new block in its entirety.
spark.storage.replication.proactive	false	Enables proactive block replication for RDD blocks. Cached RDD block replicas lost due to executor failures are replenished if there are any existing available replicas. This tries to get the replication level of the block to the initial number.

# Property name

spark.broadcast.blockSize	4m	Size of each piece of a block for TorrentBroadcastFactory. Too large a value decreases parallelism during broadcast (makes it slower); however, if it is too small, BlockManager might take a performance hit.
spark.executor.cores	1 in YARN mode, all the available cores on the worker in standalone and Mesos coarse-grained modes.	The number of cores to use on each executor. In standalone and Mesos coarse-grained modes, setting this parameter allows an application to run multiple executors on the same worker, provided that there are enough cores on that worker. Otherwise, only one executor per application will run on each worker.
spark.default.parallelism	For distributed shuffle operations like reduceByKey and join, the largest number of partitions in a parent RDD. For operations like parallelize with no parent RDDs, it depends on the cluster manager:
Local mode: number of cores on the local machine
Mesos fine grained mode: 8
Others: total number of cores on all executor nodes or 2, whichever is larger
Default number of partitions in RDDs returned by transformations like join, reduceByKey, and parallelize when not set by user.
spark.executor.heartbeatInterval	10s	Interval between each executor's heartbeats to the driver. Heartbeats let the driver know that the executor is still alive and update it with metrics for in-progress tasks. spark.executor.heartbeatInterval should be significantly less than spark.network.timeout
spark.files.fetchTimeout	60s	Communication timeout to use when fetching files added through SparkContext.addFile() from the driver.
spark.files.useFetchCache	true	If set to true (default), file fetching will use a local cache that is shared by executors that belong to the same application, which can improve task launching performance when running many executors on the same host. If set to false, these caching optimizations will be disabled and all executors will fetch their own copies of files. This optimization may be disabled in order to use Spark local directories that reside on NFS filesystems (see SPARK-6313 for more details).
spark.files.overwrite	false	Whether to overwrite files added through SparkContext.addFile() when the target file exists and its contents do not match those of the source.
spark.files.maxPartitionBytes	134217728 (128 MB)	The maximum number of bytes to pack into a single partition when reading files.
spark.files.openCostInBytes	4194304 (4 MB)	The estimated cost to open a file, measured by the number of bytes could be scanned in the same time. This is used when putting multiple files into a partition. It is better to over estimate, then the partitions with small files will be faster than partitions with bigger files.
spark.hadoop.cloneConf	false	If set to true, clones a new Hadoop Configuration object for each task. This option should be enabled to work around Configuration thread-safety issues (see SPARK-2546 for more details). This is disabled by default in order to avoid unexpected performance regressions for jobs that are not affected by these issues.
spark.hadoop.validateOutputSpecs	true	If set to true, validates the output specification (e.g. checking if the output directory already exists) used in saveAsHadoopFile and other variants. This can be disabled to silence exceptions due to pre-existing output directories. We recommend that users do not disable this except if trying to achieve compatibility with previous versions of Spark. Simply use Hadoop's FileSystem API to delete output directories by hand. This setting is ignored for jobs generated through Spark Streaming's StreamingContext, since data may need to be rewritten to pre-existing output directories during checkpoint recovery.
spark.storage.memoryMapThreshold	2m	Size of a block above which Spark memory maps when reading a block from disk. This prevents Spark from memory mapping very small blocks. In general, memory mapping has high overhead for blocks close to or below the page size of the operating system.
spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version	1	The file output committer algorithm version, valid algorithm version number: 1 or 2. Version 2 may have better performance, but version 1 may handle failures better in certain situations, as per

# Networking

Property Name	Default	Meaning
spark.rpc.message.maxSize	128	Maximum message size (in MB) to allow in "control plane" communication; generally only applies to map output size information sent between executors and the driver. Increase this if you are running jobs with many thousands of map and reduce tasks and see messages about the RPC message size.
spark.blockManager.port	(random)	Port for all block managers to listen on. These exist on both the driver and the executors.
spark.driver.blockManager.port	(value of spark.blockManager.port)	Driver-specific port for the block manager to listen on, for cases where it cannot use the same configuration as executors.
spark.driver.bindAddress	(value of spark.driver.host)	Hostname or IP address where to bind listening sockets. This config overrides the SPARK_LOCAL_IP environment variable (see below).
It also allows a different address from the local one to be advertised to executors or external systems. This is useful, for example, when running containers with bridged networking. For this to properly work, the different ports used by the driver (RPC, block manager and UI) need to be forwarded from the container's host.
spark.driver.host	(local hostname)	Hostname or IP address for the driver. This is used for communicating with the executors and the standalone Master.
spark.driver.port	(random)	Port for the driver to listen on. This is used for communicating with the executors and the standalone Master.
spark.network.timeout	120s	Default timeout for all network interactions. This config will be used in place of spark.core.connection.ack.wait.timeout, spark.storage.blockManagerSlaveTimeoutMs, spark.shuffle.io.connectionTimeout, spark.rpc.askTimeout or spark.rpc.lookupTimeout if they are not configured.
spark.port.maxRetries	16	Maximum number of retries when binding to a port before giving up. When a port is given a specific value (non 0), each subsequent retry will increment the port used in the previous attempt by 1 before retrying. This essentially allows it to try a range of ports from the start port specified to port + maxRetries.
spark.rpc.numRetries	3	Number of times to retry before an RPC task gives up. An RPC task will run at most times of this number.
spark.rpc.retry.wait	3s	Duration for an RPC ask operation to wait before retrying.
spark.rpc.askTimeout	spark.network.timeout	Duration for an RPC ask operation to wait before timing out.
spark.rpc.lookupTimeout	120s	Duration for an RPC remote endpoint lookup operation to wait before timing out.


# Scheduling


Property Name	Default	Meaning
spark.cores.max	(not set)	When running on a standalone deploy cluster or a Mesos cluster in "coarse-grained" sharing mode, the maximum amount of CPU cores to request for the application from across the cluster (not from each machine). If not set, the default will be spark.deploy.defaultCores on Spark's standalone cluster manager, or infinite (all available cores) on Mesos.
spark.locality.wait	3s	How long to wait to launch a data-local task before giving up and launching it on a less-local node. The same wait will be used to step through multiple locality levels (process-local, node-local, rack-local and then any). It is also possible to customize the waiting time for each level by setting spark.locality.wait.node, etc. You should increase this setting if your tasks are long and see poor locality, but the default usually works well.
spark.locality.wait.node	spark.locality.wait	Customize the locality wait for node locality. For example, you can set this to 0 to skip node locality and search immediately for rack locality (if your cluster has rack information).
spark.locality.wait.process	spark.locality.wait	Customize the locality wait for process locality. This affects tasks that attempt to access cached data in a particular executor process.
spark.locality.wait.rack	spark.locality.wait	Customize the locality wait for rack locality.
spark.scheduler.maxRegisteredResourcesWaitingTime	30s	Maximum amount of time to wait for resources to register before scheduling begins.
spark.scheduler.minRegisteredResourcesRatio	0.8 for YARN mode; 0.0 for standalone mode and Mesos coarse-grained mode	The minimum ratio of registered resources (registered resources / total expected resources) (resources are executors in yarn mode, CPU cores in standalone mode and Mesos coarsed-grained mode ['spark.cores.max' value is total expected resources for Mesos coarse-grained mode] ) to wait for before scheduling begins. Specified as a double between 0.0 and 1.0. Regardless of whether the minimum ratio of resources has been reached, the maximum amount of time it will wait before scheduling begins is controlled by config spark.scheduler.maxRegisteredResourcesWaitingTime.
spark.scheduler.mode	FIFO	The scheduling mode between jobs submitted to the same SparkContext. Can be set to FAIR to use fair sharing instead of queueing jobs one after another. Useful for multi-user services.
spark.scheduler.revive.interval	1s	The interval length for the scheduler to revive the worker resource offers to run tasks.
spark.blacklist.enabled	false	If set to "true", prevent Spark from scheduling tasks on executors that have been blacklisted due to too many task failures. The blacklisting algorithm can be further controlled by the other "spark.blacklist" configuration options.
spark.blacklist.timeout	1h	(Experimental) How long a node or executor is blacklisted for the entire application, before it is unconditionally removed from the blacklist to attempt running new tasks.
spark.blacklist.task.maxTaskAttemptsPerExecutor	1	(Experimental) For a given task, how many times it can be retried on one executor before the executor is blacklisted for that task.
spark.blacklist.task.maxTaskAttemptsPerNode	2	(Experimental) For a given task, how many times it can be retried on one node, before the entire node is blacklisted for that task.
spark.blacklist.stage.maxFailedTasksPerExecutor	2	(Experimental) How many different tasks must fail on one executor, within one stage, before the executor is blacklisted for that stage.
spark.blacklist.stage.maxFailedExecutorsPerNode	2	(Experimental) How many different executors are marked as blacklisted for a given stage, before the entire node is marked as failed for the stage.
spark.blacklist.application.maxFailedTasksPerExecutor	2	(Experimental) How many different tasks must fail on one executor, in successful task sets, before the executor is blacklisted for the entire application. Blacklisted executors will be automatically added back to the pool of available resources after the timeout specified by spark.blacklist.timeout. Note that with dynamic allocation, though, the executors may get marked as idle and be reclaimed by the cluster manager.
spark.blacklist.application.maxFailedExecutorsPerNode	2	(Experimental) How many different executors must be blacklisted for the entire application, before the node is blacklisted for the entire application. Blacklisted nodes will be automatically added back to the pool of available resources after the timeout specified by spark.blacklist.timeout. Note that with dynamic allocation, though, the executors on the node may get marked as idle and be reclaimed by the cluster manager.
spark.blacklist.killBlacklistedExecutors	false	(Experimental) If set to "true", allow Spark to automatically kill, and attempt to re-create, executors when they are blacklisted. Note that, when an entire node is added to the blacklist, all of the executors on that node will be killed.
spark.speculation	false	If set to "true", performs speculative execution of tasks. This means if one or more tasks are running slowly in a stage, they will be re-launched.
spark.speculation.interval	100ms	How often Spark will check for tasks to speculate.
spark.speculation.multiplier	1.5	How many times slower a task is than the median to be considered for speculation.
spark.speculation.quantile	0.75	Fraction of tasks which must be complete before speculation is enabled for a particular stage.
spark.task.cpus	1	Number of cores to allocate for each task.
spark.task.maxFailures	4	Number of failures of any particular task before giving up on the job. The total number of failures spread across different tasks will not cause the job to fail; a particular task has to fail this number of attempts. Should be greater than or equal to 1. Number of allowed retries = this value - 1.
spark.task.reaper.enabled	false	Enables monitoring of killed / interrupted tasks. When set to true, any task which is killed will be monitored by the executor until that task actually finishes executing. See the other spark.task.reaper.* configurations for details on how to control the exact behavior of this monitoring. When set to false (the default), task killing will use an older code path which lacks such monitoring.
spark.task.reaper.pollingInterval	10s	When spark.task.reaper.enabled = true, this setting controls the frequency at which executors will poll the status of killed tasks. If a killed task is still running when polled then a warning will be logged and, by default, a thread-dump of the task will be logged (this thread dump can be disabled via the spark.task.reaper.threadDump setting, which is documented below).
spark.task.reaper.threadDump	true	When spark.task.reaper.enabled = true, this setting controls whether task thread dumps are logged during periodic polling of killed tasks. Set this to false to disable collection of thread dumps.
spark.task.reaper.killTimeout	-1	When spark.task.reaper.enabled = true, this setting specifies a timeout after which the executor JVM will kill itself if a killed task has not stopped running. The default value, -1, disables this mechanism and prevents the executor from self-destructing. The purpose of this setting is to act as a safety-net to prevent runaway uncancellable tasks from rendering an executor unusable.
spark.stage.maxConsecutiveAttempts	4	Number of consecutive stage attempts allowed before a stage is aborted.


# Dynamic allocation
Property Name	Default	Meaning
spark.dynamicAllocation.enabled	false	Whether to use dynamic resource allocation, which scales the number of executors registered with this application up and down based on the workload. For more detail, see the description here.

This requires spark.shuffle.service.enabled to be set. The following configurations are also relevant: spark.dynamicAllocation.minExecutors, spark.dynamicAllocation.maxExecutors, and spark.dynamicAllocation.initialExecutors
spark.dynamicAllocation.executorIdleTimeout	60s	If dynamic allocation is enabled and an executor has been idle for more than this duration, the executor will be removed. For more detail, see this description.
spark.dynamicAllocation.cachedExecutorIdleTimeout	infinity	If dynamic allocation is enabled and an executor which has cached data blocks has been idle for more than this duration, the executor will be removed. For more details, see this description.
spark.dynamicAllocation.initialExecutors	spark.dynamicAllocation.minExecutors	Initial number of executors to run if dynamic allocation is enabled.

If `--num-executors` (or `spark.executor.instances`) is set and larger than this value, it will be used as the initial number of executors.
spark.dynamicAllocation.maxExecutors	infinity	Upper bound for the number of executors if dynamic allocation is enabled.
spark.dynamicAllocation.minExecutors	0	Lower bound for the number of executors if dynamic allocation is enabled.
spark.dynamicAllocation.schedulerBacklogTimeout	1s	If dynamic allocation is enabled and there have been pending tasks backlogged for more than this duration, new executors will be requested. For more detail, see this description.
spark.dynamicAllocation.sustainedSchedulerBacklogTimeout	schedulerBacklogTimeout	Same as spark.dynamicAllocation.schedulerBacklogTimeout, but used only for subsequent executor requests. For more detail, see this description.
### Security
Property Name	Default	Meaning
spark.acls.enable	false	Whether Spark acls should be enabled. If enabled, this checks to see if the user has access permissions to view or modify the job. Note this requires the user to be known, so if the user comes across as null no checks are done. Filters can be used with the UI to authenticate and set the user.
spark.admin.acls	Empty	Comma separated list of users/administrators that have view and modify access to all Spark jobs. This can be used if you run on a shared cluster and have a set of administrators or devs who help debug when things do not work. Putting a "*" in the list means any user can have the privilege of admin.
spark.admin.acls.groups	Empty	Comma separated list of groups that have view and modify access to all Spark jobs. This can be used if you have a set of administrators or developers who help maintain and debug the underlying infrastructure. Putting a "*" in the list means any user in any group can have the privilege of admin. The user groups are obtained from the instance of the groups mapping provider specified by spark.user.groups.mapping. Check the entry spark.user.groups.mapping for more details.
spark.user.groups.mapping	org.apache.spark.security.ShellBasedGroupsMappingProvider	The list of groups for a user are determined by a group mapping service defined by the trait org.apache.spark.security.GroupMappingServiceProvider which can configured by this property. A default unix shell based implementation is provided org.apache.spark.security.ShellBasedGroupsMappingProvider which can be specified to resolve a list of groups for a user. Note: This implementation supports only a Unix/Linux based environment. Windows environment is currently not supported. However, a new platform/protocol can be supported by implementing the trait org.apache.spark.security.GroupMappingServiceProvider.
spark.authenticate	false	Whether Spark authenticates its internal connections. See spark.authenticate.secret if not running on YARN.
spark.authenticate.secret	None	Set the secret key used for Spark to authenticate between components. This needs to be set if not running on YARN and authentication is enabled.
spark.network.crypto.enabled	false	Enable encryption using the commons-crypto library for RPC and block transfer service. Requires spark.authenticate to be enabled.
spark.network.crypto.keyLength	128	The length in bits of the encryption key to generate. Valid values are 128, 192 and 256.
spark.network.crypto.keyFactoryAlgorithm	PBKDF2WithHmacSHA1	The key factory algorithm to use when generating encryption keys. Should be one of the algorithms supported by the javax.crypto.SecretKeyFactory class in the JRE being used.
spark.network.crypto.saslFallback	true	Whether to fall back to SASL authentication if authentication fails using Spark's internal mechanism. This is useful when the application is connecting to old shuffle services that do not support the internal Spark authentication protocol. On the server side, this can be used to block older clients from authenticating against a new shuffle service.
spark.network.crypto.config.*	None	Configuration values for the commons-crypto library, such as which cipher implementations to use. The config name should be the name of commons-crypto configuration without the "commons.crypto" prefix.
spark.authenticate.enableSaslEncryption	false	Enable encrypted communication when authentication is enabled. This is supported by the block transfer service and the RPC endpoints.
spark.network.sasl.serverAlwaysEncrypt	false	Disable unencrypted connections for services that support SASL authentication.
spark.core.connection.ack.wait.timeout	spark.network.timeout	How long for the connection to wait for ack to occur before timing out and giving up. To avoid unwilling timeout caused by long pause like GC, you can set larger value.
spark.modify.acls	Empty	Comma separated list of users that have modify access to the Spark job. By default only the user that started the Spark job has access to modify it (kill it for example). Putting a "*" in the list means any user can have access to modify it.
spark.modify.acls.groups	Empty	Comma separated list of groups that have modify access to the Spark job. This can be used if you have a set of administrators or developers from the same team to have access to control the job. Putting a "*" in the list means any user in any group has the access to modify the Spark job. The user groups are obtained from the instance of the groups mapping provider specified by spark.user.groups.mapping. Check the entry spark.user.groups.mapping for more details.
spark.ui.filters	None	Comma separated list of filter class names to apply to the Spark web UI. The filter should be a standard javax servlet Filter. Parameters to each filter can also be specified by setting a java system property of:
spark.<class name of filter>.params='param1=value1,param2=value2'
For example:
-Dspark.ui.filters=com.test.filter1
-Dspark.com.test.filter1.params='param1=foo,param2=testing'
spark.ui.view.acls	Empty	Comma separated list of users that have view access to the Spark web ui. By default only the user that started the Spark job has view access. Putting a "*" in the list means any user can have view access to this Spark job.
spark.ui.view.acls.groups	Empty	Comma separated list of groups that have view access to the Spark web ui to view the Spark Job details. This can be used if you have a set of administrators or developers or users who can monitor the Spark job submitted. Putting a "*" in the list means any user in any group can view the Spark job details on the Spark web ui. The user groups are obtained from the instance of the groups mapping provider specified by spark.user.groups.mapping. Check the entry spark.user.groups.mapping for more details.
### TLS / SSL
Property Name	Default	Meaning
spark.ssl.enabled	false	Whether to enable SSL connections on all supported protocols.
When spark.ssl.enabled is configured, spark.ssl.protocol is required.
All the SSL settings like spark.ssl.xxx where xxx is a particular configuration property, denote the global configuration for all the supported protocols. In order to override the global configuration for the particular protocol, the properties must be overwritten in the protocol-specific namespace.
Use spark.ssl.YYY.XXX settings to overwrite the global configuration for particular protocol denoted by YYY. Example values for YYY include fs, ui, standalone, and historyServer. See SSL Configuration for details on hierarchical SSL configuration for services.
spark.ssl.[namespace].port	None	The port where the SSL service will listen on.
The port must be defined within a namespace configuration; see SSL Configuration for the available namespaces.
When not set, the SSL port will be derived from the non-SSL port for the same service. A value of "0" will make the service bind to an ephemeral port.
spark.ssl.enabledAlgorithms	Empty	A comma separated list of ciphers. The specified ciphers must be supported by JVM. The reference list of protocols one can find on this page. Note: If not set, it will use the default cipher suites of JVM.
spark.ssl.keyPassword	None	A password to the private key in key-store.
spark.ssl.keyStore	None	A path to a key-store file. The path can be absolute or relative to the directory where the component is started in.
spark.ssl.keyStorePassword	None	A password to the key-store.
spark.ssl.keyStoreType	JKS	The type of the key-store.
spark.ssl.protocol	None	A protocol name. The protocol must be supported by JVM. The reference list of protocols one can find on this page.
spark.ssl.needClientAuth	false	Set true if SSL needs client authentication.
spark.ssl.trustStore	None	A path to a trust-store file. The path can be absolute or relative to the directory where the component is started in.
spark.ssl.trustStorePassword	None	A password to the trust-store.
spark.ssl.trustStoreType	JKS	The type of the trust-store.

Spark Streaming
Property Name	Default	Meaning
spark.streaming.backpressure.enabled	false	Enables or disables Spark Streaming's internal backpressure mechanism (since 1.5). This enables the Spark Streaming to control the receiving rate based on the current batch scheduling delays and processing times so that the system receives only as fast as the system can process. Internally, this dynamically sets the maximum receiving rate of receivers. This rate is upper bounded by the values spark.streaming.receiver.maxRate and spark.streaming.kafka.maxRatePerPartition if they are set (see below).
spark.streaming.backpressure.initialRate	not set	This is the initial maximum receiving rate at which each receiver will receive data for the first batch when the backpressure mechanism is enabled.
spark.streaming.blockInterval	200ms	Interval at which data received by Spark Streaming receivers is chunked into blocks of data before storing them in Spark. Minimum recommended - 50 ms. See the performance tuning section in the Spark Streaming programing guide for more details.
spark.streaming.receiver.maxRate	not set	Maximum rate (number of records per second) at which each receiver will receive data. Effectively, each stream will consume at most this number of records per second. Setting this configuration to 0 or a negative number will put no limit on the rate. See the deployment guide in the Spark Streaming programing guide for mode details.
spark.streaming.receiver.writeAheadLog.enable	false	Enable write ahead logs for receivers. All the input data received through receivers will be saved to write ahead logs that will allow it to be recovered after driver failures. See the deployment guide in the Spark Streaming programing guide for more details.
spark.streaming.unpersist	true	Force RDDs generated and persisted by Spark Streaming to be automatically unpersisted from Spark's memory. The raw input data received by Spark Streaming is also automatically cleared. Setting this to false will allow the raw data and persisted RDDs to be accessible outside the streaming application as they will not be cleared automatically. But it comes at the cost of higher memory usage in Spark.
spark.streaming.stopGracefullyOnShutdown	false	If true, Spark shuts down the StreamingContext gracefully on JVM shutdown rather than immediately.
spark.streaming.kafka.maxRatePerPartition	not set	Maximum rate (number of records per second) at which data will be read from each Kafka partition when using the new Kafka direct stream API. See the Kafka Integration guide for more details.
spark.streaming.kafka.maxRetries	1	Maximum number of consecutive retries the driver will make in order to find the latest offsets on the leader of each partition (a default value of 1 means that the driver will make a maximum of 2 attempts). Only applies to the new Kafka direct stream API.
spark.streaming.ui.retainedBatches	1000	How many batches the Spark Streaming UI and status APIs remember before garbage collecting.
spark.streaming.driver.writeAheadLog.closeFileAfterWrite	false	Whether to close the file after writing a write ahead log record on the driver. Set this to 'true' when you want to use S3 (or any file system that does not support flushing) for the metadata WAL on the driver.
spark.streaming.receiver.writeAheadLog.closeFileAfterWrite	false	Whether to close the file after writing a write ahead log record on the receivers. Set this to 'true' when you want to use S3 (or any file system that does not support flushing) for the data WAL on the receivers.
### SparkR
Property Name	Default	Meaning
spark.r.numRBackendThreads	2	Number of threads used by RBackend to handle RPC calls from SparkR package.
spark.r.command	Rscript	Executable for executing R scripts in cluster modes for both driver and workers.
spark.r.driver.command	spark.r.command	Executable for executing R scripts in client modes for driver. Ignored in cluster modes.
spark.r.shell.command	R	Executable for executing sparkR shell in client modes for driver. Ignored in cluster modes. It is the same as environment variable SPARKR_DRIVER_R, but take precedence over it. spark.r.shell.command is used for sparkR shell while spark.r.driver.command is used for running R script.
spark.r.backendConnectionTimeout	6000	Connection timeout set by R process on its connection to RBackend in seconds.
spark.r.heartBeatInterval	100	Interval for heartbeats sent from SparkR backend to R process to prevent connection timeout.
### GraphX
Property Name	Default	Meaning
spark.graphx.pregel.checkpointInterval	-1	Checkpoint interval for graph and message in Pregel. It used to avoid stackOverflowError due to long lineage chains after lots of iterations. The checkpoint is disabled by default.
### Deploy
Property Name	Default	Meaning
spark.deploy.recoveryMode	NONE	The recovery mode setting to recover submitted Spark jobs with cluster mode when it failed and relaunches. This is only applicable for cluster mode when running with Standalone or Mesos.
spark.deploy.zookeeper.url	None	When `spark.deploy.recoveryMode` is set to ZOOKEEPER, this configuration is used to set the zookeeper URL to connect to.
spark.deploy.zookeeper.dir	None	When `spark.deploy.recoveryMode` is set to ZOOKEEPER, this configuration is used to set the zookeeper directory to store recovery state