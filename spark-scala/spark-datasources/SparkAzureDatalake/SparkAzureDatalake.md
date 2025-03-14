# Azure Blob location
    
    wasbs://<containerName>@<StorageAccount>.blob.core.windows.net/<pathAfterContainerName>

# Azure ADLS location
    
    source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/folder1"
    
# Running jar in spark-submit
    
    spark-submit `
        --class com.mahfooz.spark.adls.adls.key.SparkAzureAdlsKey `
        --master "local[*]" `
        target/scala-2.13/SparkAzureDatalake-assembly-1.0.jar `
        "hello"

