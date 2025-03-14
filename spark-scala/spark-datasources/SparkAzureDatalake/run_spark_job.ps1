Write-Output "Running spark submit"
spark-submit `
    --class com.mahfooz.spark.adls.adls.key.SparkAzureAdlsKey `
    --master "local[*]" `
    target/scala-2.13/SparkAzureDatalake-assembly-1.0.jar `
    "hello"
Write-Output "Removing temp directory"
Remove-Item -Recurse -Path C:\Users\Mohammed_Alam\AppData\Local\Temp\spark* -Force
Write-Output "Spark successfully completed"
