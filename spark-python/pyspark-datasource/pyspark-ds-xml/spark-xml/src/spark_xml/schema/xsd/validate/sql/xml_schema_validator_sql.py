import os


from spark_xml.util.session.spark_session_util import get_spark_session

if __name__ == "__main__":
    spark =get_spark_session(
        app_name="spark-db-xml-attribute",
        scala_version="2.12",
        spark_xml_version="0.18.0")

    # data path
    data_home = os.environ["DATA_HOME"]
    xml_file = os.path.join(data_home, "file_data", "xml", "orders.xml")
    xml_xsd = os.path.join(data_home, "file_data", "xml", "orders.xsd")

    print(xml_file)

    # adding spark context
    spark.sparkContext.addFile(xml_xsd)


    rootTag = "Root"
    rowTag = "Root"

    spark.sql(
        f"""
        create table orders
        USING xml
            OPTIONS (path "{xml_file}",
            rowTag "{rowTag}",
            inferSchema "false",
            rowValidationXSDPath "orders.xsd",
            rootTag "{rootTag}")
    """
    )
    spark.sql("select * from orders").printSchema()
