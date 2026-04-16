---
applyTo: "**/*.py"
---

# Azure Storage Instructions

## Protocol

Use `abfss://` (ADLS Gen2 with TLS) for all new code. Legacy `wasbs://` is for
Blob Storage only.

## Path Format

```
abfss://<CONTAINER>@<ACCOUNT>.dfs.core.windows.net/<PATH>
```

## JARs

```python
hadoop_azure = "3.3.4"
.config("spark.jars.packages", f"org.apache.hadoop:hadoop-azure:{hadoop_azure}")
```

## Authentication Methods

### Account Key

```python
.config(f"spark.hadoop.fs.azure.account.key.{account_name}.dfs.core.windows.net",
        account_key)
```

### Service Principal (OAuth 2.0)

```python
prefix = "spark.hadoop.fs.azure.account"
suffix = f"{account_name}.dfs.core.windows.net"
.config(f"{prefix}.auth.type.{suffix}", "OAuth")
.config(f"{prefix}.oauth.provider.type.{suffix}",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
.config(f"{prefix}.oauth2.client.id.{suffix}", client_id)
.config(f"{prefix}.oauth2.client.secret.{suffix}", client_secret)
.config(f"{prefix}.oauth2.client.endpoint.{suffix}",
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
```

### SAS Token

```python
.config(f"spark.hadoop.fs.azure.sas.token.provider.type.{suffix}",
        "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
.config(f"spark.hadoop.fs.azure.sas.fixed.token.{suffix}", sas_token)
```

## Environment Variables

```bash
AZURE_STORAGE_ACCOUNT
AZURE_STORAGE_KEY
AZURE_CLIENT_ID
AZURE_CLIENT_SECRET
AZURE_TENANT_ID
AZURE_SAS_TOKEN
AZURE_CONTAINER
```
