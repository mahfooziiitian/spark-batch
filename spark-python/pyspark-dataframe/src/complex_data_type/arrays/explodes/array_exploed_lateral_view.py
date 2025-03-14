"""
LATERAL VIEW [ OUTER ] generator_function ( expression [, ...] ) [ table_identifier ] AS column_identifier [, ...]
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, IntegerType
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"
if __name__ == "__main__":
    # create an app named linuxhint
    spark_app = SparkSession.builder.appName('array_explode').getOrCreate()

    # consider an array with 5 elements
    my_array_data = [(1, ['A']), (2, ['B', 'L', 'B']), (3, ['K', 'A', 'K']),
                     (4, ['K']), (3, ['B', 'P']), (5, [])]

    # define the StructType and StructFields
    # for the above data
    schema = StructType([StructField("Student_category", IntegerType()),
                         StructField("Student_full_name", ArrayType(StringType()))])

    # create the dataframe and add schema to the dataframe
    df = spark_app.createDataFrame(my_array_data, schema=schema)

    # apply to explode on the Student_full_name column
    df.select("Student_category", "Student_full_name", explode('Student_full_name')).show()
# end main
