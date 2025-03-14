import xml.etree.ElementTree as Et

import requests
from pyspark.sql import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StringType, StructField


def select_text(xml_doc, xpath):
    nodes = [e.text for e in xml_doc.findall(xpath) if isinstance(e, Et.Element)]
    return next(iter(nodes), None)


def extract_cd_info(payload):
    xml_doc = Et.fromstring(payload)
    return {
        'title': select_text(xml_doc, 'TITLE'),
        'artist': select_text(xml_doc, 'ARTIST')
    }


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("xml_data").getOrCreate()

    cds_url = 'https://www.w3schools.com/xml/cd_catalog.xml'

    # download data
    cds_txt = requests.get(cds_url).text
    print(cds_txt)

    # convert to XML
    doc = Et.fromstring(cds_txt)

    # extract CD XML
    utf8 = 'utf-8'
    cds = [Et.tostring(x, encoding=utf8) for x in doc.findall('CD')]

    # create dataframe
    normalizedCds = [str(cd, utf8).strip() for cd in cds]
    rows = [Row(index=index, cd=cd) for index, cd in enumerate(normalizedCds)]
    cd_df = spark.createDataFrame(rows)

    cd_df.show(truncate=False)

    extract_cd_info_schema = StructType([
        StructField("title", StringType(), True),
        StructField("artist", StringType(), True)
    ])

    extract_cd_info_udf = udf(extract_cd_info, extract_cd_info_schema)

    (cd_df
     .withColumn("info", extract_cd_info_udf('cd'))
     .select('index', 'info.artist', 'info.title')
     .show(10, False))
