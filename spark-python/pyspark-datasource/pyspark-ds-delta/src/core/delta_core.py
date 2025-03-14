from delta.tables import *

if __name__ == '__main__':
    spark = (SparkSession.builder.appName("delta_core").master("local[*]")
             .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())

