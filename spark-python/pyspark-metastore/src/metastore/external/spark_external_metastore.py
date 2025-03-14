from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = (SparkSession.builder()
             .appName("SparkSQLExample")
             .config("javax.jdo.option.ConnectionURL", "jdbc:mysql://localhost:3306/my_metastore_db")
             .getOrCreate())
