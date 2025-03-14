from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import udf
import xml.etree.ElementTree as ET


# Function to parse XML
def parse_xml(xml_str):
    if xml_str is None:
        return None
    try:
        root = ET.fromstring(xml_str)
        return (
            root.find('title').text,
            root.find('author').text,
            float(root.find('price').text)
        )
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return None


if __name__ == '__main__':
    # Initialize Spark session
    spark = SparkSession.builder.appName("XML Column Handling").getOrCreate()

    # Sample data
    data = [
        (1, '<book><title>Effective Java</title><author>Joshua Bloch</author><price>45.00</price></book>'),
        (2, '<book><title>Clean Code</title><author>Robert C. Martin</author><price>50.00</price></book>')
    ]

    columns = ["id", "xml_data"]

    # Create DataFrame
    df = spark.createDataFrame(data, columns)
    df.show(truncate=False)

    # Define schema for the XML data
    xml_schema = StructType([
        StructField("title", StringType(), True),
        StructField("author", StringType(), True),
        StructField("price", FloatType(), True)
    ])

    # Register the UDF
    parse_xml_udf = udf(parse_xml, xml_schema)

    # Apply the UDF to parse the XML column
    parsed_df = df.withColumn("parsed_data", parse_xml_udf(df["xml_data"]))

    # Select the original columns and the new parsed columns
    result_df = parsed_df.select("id", "xml_data", "parsed_data.*")
    result_df.show(truncate=False)

