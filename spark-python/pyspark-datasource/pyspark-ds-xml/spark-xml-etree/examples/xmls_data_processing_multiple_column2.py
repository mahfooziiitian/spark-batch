import xml.etree.ElementTree as Et

from pyspark.sql import *
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import ArrayType, IntegerType


@udf
def extract_ab(xml):
    xml_doc = Et.fromstring(xml)
    return [xml_doc.attrib['a'], xml_doc.attrib['b']]


def extract_rid(xml):
    doc = Et.fromstring(xml)
    records = doc.findall('records/record')
    ids = []
    for r in records:
        ids.append(int(r.attrib["id"]))
    return ids


if __name__ == '__main__':
    appName = "xml_data_processing_multiple_column"
    master = "local[*]"

    # Create Spark session
    spark = SparkSession.builder \
        .appName(appName) \
        .master(master) \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    data = [
        {'id': 1, 'data': """<test a="100" b="200">
            <records>
                <record id="101" />
                <record id="201" />
            </records>
        </test>
    """},
        {'id': 2, 'data': """<test a="200" b="400">
            <records>
                <record id="202" />
                <record id="402" />
            </records>
        </test>
    """}]

    df = spark.createDataFrame(data)
    print(df.schema)
    df.show()

    schema = ArrayType(IntegerType())
    udf_extract_rid = udf(extract_rid, schema)

    df = df.withColumn('rids', udf_extract_rid(df["data"]))
    print(df.schema)
    df.show()

    # Explode array column
    df.withColumn('rid', explode(df['rids'])).show()

