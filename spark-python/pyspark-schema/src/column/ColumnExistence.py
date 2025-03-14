from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DataType

if __name__ == '__main__':
    # create an app named array schema fields
    spark_app = SparkSession.builder.appName('array-schema-fields').getOrCreate()

    # create student data with 5 rows and 6 attributes
    students = [['001', 'sravan', [23, 5.79, 67], 'guntur'],
                ['002', 'ojaswi', [16, 3.79, 34], 'hyd'],
                ['003', 'gnanesh chowdary', [7, 2.79, 17], 'patna'],
                ['004', 'rohith', [9, 3.69, 28], 'hyd'],
                ['005', 'sridevi', [37, 5.59, 54], 'hyd']]

    # define the StructType and StructFields
    # for the below column names
    schema = StructType([
        StructField("rollno", StringType(), True),
        StructField("name", StringType(), True),
        StructField("data", StructType([
            StructField("age", IntegerType(), True),
            StructField("height", FloatType(), True),
            StructField("weight", IntegerType(), True)])),
        StructField("address", StringType(), True)
    ])

    # create the dataframe and add schema to the dataframe
    df = spark_app.createDataFrame(students, schema=schema)

    print("name" in df.schema.fieldNames())
    print(df.schema.fields.__contains__(StructField("name", StringType(), True)))
