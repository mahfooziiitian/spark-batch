"""Parse an XML string column into a struct using Spark 4's built-in from_xml."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import from_xml, lit, schema_of_xml

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("spark-xml").getOrCreate()
    xml = [
        (
            "url",
            """<Level_0 Id0="Id0_value_file1">
        <Level_1 Id1_1 ="Id3_value" Id_2="Id2_value">
          <Level_2_A>A</Level_2_A>
          <Level_2>
            <Level_3>
              <Level_4>
                <Date>2021-01-01</Date>
                <Value>4_1</Value>
              </Level_4>
              <Level_4>
                <Date>2021-01-02</Date>
                <Value>4_2</Value>
              </Level_4>
            </Level_3>
          </Level_2>
        </Level_1>
      </Level_0>""",
        ),
        (
            "url",
            """<Level_0 Id0="Id0_value_file2">
        <Level_1 Id1_1 ="Id3_value" Id_2="Id2_value">
          <Level_2_A>A</Level_2_A>
          <Level_2>
            <Level_3>
              <Level_4>
                <Date>2021-01-01</Date>
                <Value>4_1</Value>
              </Level_4>
              <Level_4>
                <Date>2021-01-02</Date>
                <Value>4_2</Value>
              </Level_4>
            </Level_3>
          </Level_2>
        </Level_1>
      </Level_0>""",
        ),
    ]

    rdd = spark.sparkContext.parallelize(xml)
    df = spark.createDataFrame(rdd, "url string, content string")

    # Infer the XML schema from a representative row, then parse the column.
    options = {"rowTag": "Level_0"}
    sample = df.select("content").first()["content"]
    payloadSchema = df.select(schema_of_xml(lit(sample), options)).first()[0]

    # parse xml
    parsed = df.withColumn("parsed", from_xml(df.content, payloadSchema, options))

    parsed.printSchema()

    # select required data
    df2 = parsed.select(
        "parsed._Id0",
        f.explode_outer("parsed.Level_1.Level_2.Level_3.Level_4").alias("Level_4"),
    )
    df2.printSchema()

    df2.select("_Id0", "Level_4.*")
