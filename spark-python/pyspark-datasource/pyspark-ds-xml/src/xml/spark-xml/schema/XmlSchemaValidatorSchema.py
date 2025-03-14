import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, TimestampType, DoubleType

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]") \
        .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0') \
        .appName("spark-xml-validator").getOrCreate()

    # data path
    dataHome = os.environ["DATA_HOME"]
    xmlFile = dataHome + "\\FileData\\Xml\\orders.xml"
    xmlXsd = dataHome + "\\FileData\\Xml\\orders.xsd"

    # adding spark context
    spark.sparkContext.addFile(xmlXsd)
    schema = StructType([StructField('Customers', StructType([StructField('Customer', ArrayType(StructType(
        [StructField('CompanyName', StringType(), True), StructField('ContactName', StringType(), True),
         StructField('ContactTitle', StringType(), True), StructField('Fax', StringType(), True),
         StructField('FullAddress', StructType(
             [StructField('Address', StringType(), True), StructField('City', StringType(), True),
              StructField('Country', StringType(), True), StructField('PostalCode', LongType(), True),
              StructField('Region', StringType(), True)]), True), StructField('Phone', StringType(), True),
         StructField('_CustomerID', StringType(), True)]), True), True)]), True), StructField('Orders', StructType([
        StructField(
            'Order',
            ArrayType(
                StructType(
                    [
                        StructField(
                            'CustomerID',
                            StringType(),
                            True),
                        StructField(
                            'EmployeeID',
                            LongType(),
                            True),
                        StructField(
                            'OrderDate',
                            TimestampType(),
                            True),
                        StructField(
                            'RequiredDate',
                            TimestampType(),
                            True),
                        StructField(
                            'ShipInfo',
                            StructType(
                                [
                                    StructField(
                                        'Freight',
                                        DoubleType(),
                                        True),
                                    StructField(
                                        'ShipAddress',
                                        StringType(),
                                        True),
                                    StructField(
                                        'ShipCity',
                                        StringType(),
                                        True),
                                    StructField(
                                        'ShipCountry',
                                        StringType(),
                                        True),
                                    StructField(
                                        'ShipName',
                                        StringType(),
                                        True),
                                    StructField(
                                        'ShipPostalCode',
                                        LongType(),
                                        True),
                                    StructField(
                                        'ShipRegion',
                                        StringType(),
                                        True),
                                    StructField(
                                        'ShipVia',
                                        LongType(),
                                        True),
                                    StructField(
                                        '_ShippedDate',
                                        TimestampType(),
                                        True)]),
                            True)]),
                True),
            True)]),
                                                                                              True)])
    #
    orders = (spark.read.
              format("com.databricks.spark.xml")
              .option("rowTag", "Root")
              .schema(schema)  # .option("rowValidationXSDPath", "orders.xsd").
              .load(xmlFile))

    orders.printSchema()

    print(orders.schema)
