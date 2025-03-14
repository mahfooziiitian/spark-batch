"""
create table kafka_offset2 (job_id int,job_name varchar2(200),details clob)

INSERT INTO kafka_offset2 (JOB_ID, JOB_NAME, DETAILS)
VALUES 
    (1, 'Software Developer', '{"skills": ["Python", "SQL", "Spark"], "experience": 5, "location": "Remote"}'),
    (2, 'Data Analyst', '{"skills": ["Excel", "SQL", "Tableau"], "experience": 3, "location": "New York"}'),
    (3, 'Project Manager', '{"skills": ["Agile", "Scrum", "Leadership"], "experience": 7, "location": "San Francisco"}');

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

    query = "select * from kafka_offset2"
    reader_properties = {
        "url": config_reader.get_url(),
        "user": config_reader.get_user(),
        "password": config_reader.get_password(),
        "driver": config_reader.get_driver(),
        "query": query,
    }

    df = spark.read.format("jdbc").options(**reader_properties).load()

    df.show(truncate=False)
