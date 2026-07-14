# HDFS and Spark

PySpark examples for reading and writing data to **Hadoop Distributed File System (HDFS)**
using the `hdfs://` protocol.

## Prerequisites

- Java 11
- PySpark 3.5.x
- Access to an HDFS cluster (NameNode reachable)

## Library

HDFS support is bundled with Spark — no extra JARs required.
Spark ships with `hadoop-client` which includes the HDFS connector.

## Authentication Methods

### Simple (no security)

Default mode — no extra config needed. HDFS uses the OS username of the Spark process.

```python
spark = (SparkSession.builder
         .appName("hdfs-demo")
         .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
         .getOrCreate())
```

### Kerberos

```python
.config("spark.hadoop.hadoop.security.authentication", "kerberos")
.config("spark.hadoop.dfs.namenode.kerberos.principal",
        "nn/_HOST@REALM")
```

Before launching the job:

```bash
kinit -kt /path/to/keytab principal@REALM
```

## HA (High Availability) NameNode

```python
.config("spark.hadoop.dfs.nameservices", "mycluster")
.config("spark.hadoop.dfs.ha.namenodes.mycluster", "nn1,nn2")
.config("spark.hadoop.dfs.namenode.rpc-address.mycluster.nn1",
        "namenode1:8020")
.config("spark.hadoop.dfs.namenode.rpc-address.mycluster.nn2",
        "namenode2:8020")
.config("spark.hadoop.dfs.client.failover.proxy.provider.mycluster",
        "org.apache.hadoop.hdfs.server.namenode.ha"
        ".ConfiguredFailoverProxyProvider")
```

## Path Format

```
hdfs://<NAMENODE>:<PORT>/<PATH>
```

Or with HA:

```
hdfs://mycluster/<PATH>
```
