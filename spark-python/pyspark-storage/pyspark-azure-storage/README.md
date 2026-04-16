# Azure Storage and Spark

PySpark examples for reading and writing data to **Azure Blob Storage** and
**Azure Data Lake Storage Gen2** using the `abfss://` protocol.

## Prerequisites

- Java 11
- PySpark 3.5.x
- Azure storage account with access credentials

## Library

Azure storage access depends on:

1. `hadoop-azure` JAR
2. `azure-storage` JAR (pulled transitively)

The `hadoop-azure` version must match the Hadoop version bundled with Spark.

## Protocols

| Protocol | Description |
|----------|-------------|
| `wasbs://` | Azure Blob Storage (legacy) |
| `abfs://` | Azure Data Lake Storage Gen2 (without TLS) |
| `abfss://` | Azure Data Lake Storage Gen2 (with TLS, recommended) |

## Authentication Methods

### Storage Account Key

```python
spark = (SparkSession.builder
         .appName("azure-storage-demo")
         .config("spark.jars.packages",
                 "org.apache.hadoop:hadoop-azure:3.3.4")
         .config("spark.hadoop.fs.azure.account.key.<ACCOUNT>.dfs.core.windows.net",
                 "<ACCOUNT_KEY>")
         .getOrCreate())
```

### Service Principal (OAuth 2.0)

```python
.config("spark.hadoop.fs.azure.account.auth.type.<ACCOUNT>.dfs.core.windows.net",
        "OAuth")
.config("spark.hadoop.fs.azure.account.oauth.provider.type.<ACCOUNT>.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
.config("spark.hadoop.fs.azure.account.oauth2.client.id.<ACCOUNT>.dfs.core.windows.net",
        "<CLIENT_ID>")
.config("spark.hadoop.fs.azure.account.oauth2.client.secret.<ACCOUNT>.dfs.core.windows.net",
        "<CLIENT_SECRET>")
.config("spark.hadoop.fs.azure.account.oauth2.client.endpoint.<ACCOUNT>.dfs.core.windows.net",
        "https://login.microsoftonline.com/<TENANT_ID>/oauth2/token")
```

### SAS Token

```python
.config("spark.hadoop.fs.azure.sas.token.provider.type.<ACCOUNT>.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
.config("spark.hadoop.fs.azure.sas.fixed.token.<ACCOUNT>.dfs.core.windows.net",
        "<SAS_TOKEN>")
```

## Path Format

```
abfss://<CONTAINER>@<ACCOUNT>.dfs.core.windows.net/<PATH>
```
