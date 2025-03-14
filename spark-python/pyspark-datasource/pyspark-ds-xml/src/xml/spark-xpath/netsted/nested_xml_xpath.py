import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = (SparkSession.builder.master("local[*]").appName("xml_data").getOrCreate())
    xmlFile = os.environ["DATA_HOME"] + "\\FileData\\Xml\\nested_xml.xml"

    df = spark.read.text(paths=xmlFile, wholetext=True)

    df.createOrReplaceTempView("policy_center")

    spark.sql("""
    select xpath_string(value,'DWHBatch/Header/BatchId') as batchId,
     xpath_string(value,'DWHBatch/Header/TotalNoOfRecords') as TotalNoOfRecords,
     xpath_string(value,'DWHBatch/Records/Issuance/Entry/*') as issuance
     from policy_center
    """).show(truncate=False)
