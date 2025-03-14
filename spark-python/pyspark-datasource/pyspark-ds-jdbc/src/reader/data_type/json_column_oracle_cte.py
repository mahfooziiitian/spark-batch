"""
create table kafka_offset (job_id int,job_name varchar2(200),details json)

INSERT INTO kafka_offset (JOB_ID, JOB_NAME, DETAILS)
VALUES 
    (1, 'magna_table_copy', '{
    "topics_arr": [
        {
            "tp_nm": "Topic_name1",
            "partitions": [
                {
                    "id": "0",
                    "offset": 20
                },
                {
                    "id": "1",
                    "offset": 24
                }
            ]
        },
        {
            "tp_nm": "Topic_name2",
            "partitions": [
                {
                    "id": "0",
                    "offset": 20
                },
                {
                    "id": "1",
                    "offset": 24
                }
            ]
        }
    ]
}')
"""

import os

from utils.config_reader import ConfigReader
from utils.spark_util import get_spark_session

if __name__ == "__main__":
    oracle_version = "23.5.0.24.07"
    packages = [f"com.oracle.database.jdbc:ojdbc8:{oracle_version}"]

    configs = {
        "spark.app.name": "MySparkApp",
        "spark.master": "local[*]",
        "spark.jars.packages": ",".join(packages),
    }
    spark = get_spark_session(configs)
    db_config = os.environ["DATA_HOME"] + "\\Database\\Config\\Oracle\\db.conf"
    config_reader = ConfigReader(db_config)

    query = """WITH JSON_DATA AS (
            SELECT * 
            FROM
                KAFKA_OFFSET
        ), PARSED_DATA AS (
            SELECT
                JT.TOPIC_NAME,
                JT.PARTITION_ID,
                JT.PARTITION_OFFSET
            FROM
                JSON_DATA JD,
                JSON_TABLE(JD.DETAILS,
                '$.topics_arr[*]' COLUMNS ( TOPIC_NAME VARCHAR2(50) PATH '$.tp_nm',
                NESTED PATH '$.partitions[*]' COLUMNS ( PARTITION_ID VARCHAR2(10) PATH '$.id',
                PARTITION_OFFSET NUMBER PATH '$.offset' ) ) ) JT         
        )
        SELECT
            JSON_OBJECTAGG(KEY TOPIC_NAME VALUE JSON_OBJECTAGG(KEY PARTITION_ID VALUE CAST(PARTITION_OFFSET+1 AS INT) RETURNING VARCHAR2)) AS STARTING_OFFSETS,
            1 AS ID
        FROM
            PARSED_DATA
        GROUP BY
            TOPIC_NAME
        UNION
        SELECT
            '{"DOMAIN-DATA-MVNO-LINE-PROD": "LATEST"}' AS STARTING_OFFSETS,
            -1 AS ID
        FROM
            DUAL"""
    reader_properties = {
        "url": config_reader.get_url(),
        "user": config_reader.get_user(),
        "password": config_reader.get_password(),
        "driver": config_reader.get_driver(),
        "query": query,
    }

    df = spark.read.format("jdbc").options(**reader_properties).load()

    df.show(truncate=False)
