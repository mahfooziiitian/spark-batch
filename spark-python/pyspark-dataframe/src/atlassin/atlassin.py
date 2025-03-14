from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, when, expr


class CouncilsJob:

    def __init__(self):
        self.spark_session = (SparkSession.builder
                              .master("local[*]")
                              .appName("EnglandCouncilsJob")
                              .getOrCreate())
        self.input_directory = "data"

    def extract_councils(self):
        return (self.spark_session.read.
                format("csv").
                option("header", True).
                load(f"{self.input_directory}/england_councils/")
                ).withColumn("council_type", when(input_file_name().contains("district_councils"), "District Council")
                             .when(input_file_name().contains("london_boroughs"), "London Borough")
                             .when(input_file_name().contains("metropolitan_districts"), "Metropolitan District")
                             .when(input_file_name().contains("unitary_authorities"), "Unitary Authority")
                             .otherwise("Others"))

    def extract_avg_price(self):
        return (self.spark_session.read.
                format("csv").
                option("header", True).
                load(f"{self.input_directory}/property_avg_price.csv")
                .withColumnRenamed("local_authority", "council")
                .select("council",expr("cast(avg_price_nov_2019 as double) as avg_price_nov_2019")))
    def extract_sales_volume(self):
        return (self.spark_session.read.
                format("csv").
                option("header", True).
                load(f"{self.input_directory}/property_sales_volume.csv")
                .withColumnRenamed("local_authority", "council")
                .select("council", expr("cast(sales_volume_sep_2019 as double) as sales_volume_sep_2019")))

    def transform(self, councils_df, avg_price_df, sales_volume_df):
        council_price_df = councils_df.join(avg_price_df, councils_df.council == avg_price_df.council,
                                            "left_outer").select(councils_df.council, councils_df.county,
                                                                 councils_df.council_type,
                                                                 avg_price_df.avg_price_nov_2019)
        council_price_sales_df = council_price_df.join(sales_volume_df,
                                                       council_price_df.council == sales_volume_df.council,
                                                       "left_outer").select(council_price_df.council,
                                                                            councils_df.county,
                                                                            council_price_df.council_type,
                                                                            council_price_df.avg_price_nov_2019,
                                                                            sales_volume_df.sales_volume_sep_2019)
        return council_price_sales_df

    def run(self):
        return self.transform(self.extract_councils(),
                              self.extract_avg_price(),
                              self.extract_sales_volume())


if __name__ == '__main__':
    councilJob = CouncilsJob()
    df = councilJob.extract_avg_price()
    df.printSchema()
    df = councilJob.run()
    df.where("council='Durham'").show(truncate=False)
    print(df.count())
