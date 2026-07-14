---
applyTo: "**/*.py"
---

# HDFS Storage Instructions

## Protocol

Use `hdfs://` for all HDFS access. No extra JARs needed — Spark bundles
`hadoop-client`.

## Path Formats

```
hdfs://<NAMENODE>:<PORT>/<PATH>     # single NameNode
hdfs://<NAMESERVICE>/<PATH>         # HA cluster
```

## Default FS

```python
.config("spark.hadoop.fs.defaultFS", f"hdfs://{namenode}:8020")
```

## HA (High Availability) NameNode

```python
.config("spark.hadoop.dfs.nameservices", "mycluster")
.config("spark.hadoop.dfs.ha.namenodes.mycluster", "nn1,nn2")
.config("spark.hadoop.dfs.namenode.rpc-address.mycluster.nn1", "namenode1:8020")
.config("spark.hadoop.dfs.namenode.rpc-address.mycluster.nn2", "namenode2:8020")
.config("spark.hadoop.dfs.client.failover.proxy.provider.mycluster",
        "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
```

## Authentication

- **Simple** (default) — uses the OS user of the Spark process.
- **Kerberos** — run `kinit` before launching the job, then set:

```python
.config("spark.hadoop.hadoop.security.authentication", "kerberos")
```

## Environment Variables

```bash
HDFS_NAMENODE       # e.g. namenode:8020
HDFS_NN1            # HA node 1
HDFS_NN2            # HA node 2
KRB5_KTNAME         # Kerberos keytab path
```
