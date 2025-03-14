import os
import sys

from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk1.8.0_251"
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == '__main__':
    print(os.environ['JAVA_HOME'])
    print(os.environ["PYSPARK_PYTHON"])
    spark = (SparkSession.builder
             .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0')
             .master("local[*]")
             .appName("spark-xml")
             .getOrCreate())

    filePath = os.environ["DATA_HOME"] + "\\FileData\\Xml\\IdentUebersetzungen.xml"

    df = (spark.read.format("xml")
          .option("rowTag", "IdentUebersetzung")
          .option("rootTag", "IdentUebersetzung")
          .option("attributePrefix", "Attr_")
          .load(filePath))

    df.printSchema()

    newDf = df.selectExpr("Attr_IdentUebersetzungName", "explode(Lables.Lable) as Lable")

    newDf.selectExpr("Attr_IdentUebersetzungName as IdentUebersetzungName",
                     "Lable.Attr_LableName as LableName",
                     "Lable.Attr_ServiceShortName as ServiceShortName").show(truncate=False)