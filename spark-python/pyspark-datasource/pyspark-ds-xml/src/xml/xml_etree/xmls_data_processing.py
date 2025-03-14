import xml.etree.ElementTree as ET

import requests
from pyspark.sql import *
from pyspark.sql.functions import col, udf


def extract_title(payload):
    docs = ET.fromstring(payload)
    result = [e.text for e in docs.findall('TITLE') if isinstance(e, ET.Element)]
    return next(iter(result), None)


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("xml_data").getOrCreate()

    cds_url = 'https://www.w3schools.com/xml/cd_catalog.xml'

    # download data
    cds_txt = requests.get(cds_url).text
    print(cds_txt)

    # convert to XML
    doc = ET.fromstring(cds_txt)

    # extract CD XML
    utf8 = 'utf-8'
    cds = [ET.tostring(x, encoding=utf8) for x in doc.findall('CD')]
    print(cds)

    # create dataframe
    normalizedCds = [str(cd, utf8).strip() for cd in cds]
    rows = [Row(index=index, cd=cd) for index, cd in enumerate(normalizedCds)]
    cd_df = spark.createDataFrame(rows)

    cd_df.show(truncate=False)

    extract_title_udf = udf(lambda col_data: extract_title(col_data))

    cd_df.select("index", extract_title_udf(col('cd')).alias('title')).show(10, False)
