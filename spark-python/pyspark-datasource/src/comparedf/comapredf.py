import logging
import sys
from functools import reduce
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import (lower, col, udf, lit, concat_ws, md5, current_timestamp)
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import pandas as pd

# from HeliosUtils import helios_utilities as hu


class ReconComparator:
    # Initialize ReconComparator Class
    def __init__(self, spark=None, srcdf=None, tgtdf=None, key_columns_dict=None):
        self.spark = spark
        self.srcdf = srcdf
        self.tgtdf = tgtdf
        self.key_columns_dict = key_columns_dict
        self.src_cols = self.srcdf.columns
        self.tgt_cols = self.tgtdf.columns
        self.rename_src_cols = ["df1_" + x.lower() for x in self.src_cols]
        self.rename_tgt_cols = ["df2_" + x.lower() for x in self.tgt_cols]

    def recon_compare3(self, srcdf=None, tgtdf=None, key_columns_dict=None):
        try:
            src_df = srcdf if srcdf else self.srcdf
            tgt_df = tgtdf if tgtdf else self.tgtdf

            src_keylist = []
            tgt_keylist = []
            for x in self.key_columns_dict.split(","):
                a = "df1_" + x if x else "df1_" + self.key_columns_dict
                src_keylist.append(a)
                b = "df2_" + x if x else "df2_" + self.key_columns_dict
                tgt_keylist.append(b)
            source_key_columns = ','.join(src_keylist)
            tgt_key_columns = ','.join(tgt_keylist)

            # Create joining columns
            logging.info("Started creating joining condition")
            prop_dict = {"source_join_cols": source_key_columns,
                         "target_join_cols": tgt_key_columns}
            source_join_cols = prop_dict["source_join_cols"]
            target_join_cols = prop_dict["target_join_cols"]

            source_side_join_cols_arr = list(zip(source_join_cols.split(","), target_join_cols.split(",")))
            print(source_side_join_cols_arr)

            for srccolname in src_df.columns:
                src_df = src_df.withColumnRenamed(srccolname, "df1_" + srccolname)

            for tgtcolname in tgt_df.columns:
                tgt_df = tgt_df.withColumnRenamed(tgtcolname, "df2_" + tgtcolname)

            src_hash_df = src_df.withColumn("src_hash_id", md5(concat_ws(",", *self.rename_src_cols)))
            tgt_hash_df = tgt_df.withColumn("tgt_hash_id", md5(concat_ws(",", *self.rename_tgt_cols)))

            print("hashing df")
            # hu_obj = hu.HeliosUtils()
            join_condition = src_hash_df.df1_id == tgt_hash_df.df2_id
            print("Completed creating joining condition")

            print("Started Bucket2 and Bucket3 join")
            src_hash_df = src_df.withColumn("src_hash_id", md5(concat_ws(",", *self.rename_src_cols)))
            tgt_hash_df = tgt_df.withColumn("tgt_hash_id", md5(concat_ws(",", *self.rename_tgt_cols)))

            cols_to_drop = ['src_hash_id', 'tgt_hash_id']
            bucket12 = src_hash_df.join(tgt_hash_df, join_condition, 'inner')
            bucket2 = bucket12.filter((src_hash_df.src_hash_id != tgt_hash_df.tgt_hash_id)).drop(
                *cols_to_drop).withColumn("bucket_name", lit("bucket2"))

            bucket3 = bucket12.filter((src_hash_df.src_hash_id != tgt_hash_df.tgt_hash_id)).drop(
                *cols_to_drop).withColumn("bucket_name", lit("bucket3"))
            print("Completed Bucket2 and Bucket3 join")

            # bucket3.createOrReplaceTempView("bucket3")

            for srccolname in self.src_cols:
                print(srccolname)
                bucket3 = bucket3.withColumn(srccolname,
                    F.when(col("df1_"+srccolname) != col("df2_"+srccolname), lit(srccolname))
                .otherwise(lit(None)))

            bucket3 = bucket3.withColumn("concated_column",F.concat_ws(",",*self.src_cols)).drop(*self.src_cols)
            bucket3 = bucket3.withColumn("concated_column_output",
                                         F.when(col("concated_column").isNotNull(), concat_ws(" ",col("concated_column"), lit("have not matched")))
                                         .otherwise(lit("All columns have matched"))).drop("concated_column")
            bucket3.show(truncate=False)


        except Exception as error:
            logging.error(f"Error while processing recon_compare function {error}")
            raise Exception from error
            sys.exit(101)

    def recon_compare31(self, bucket3):
        try:
            for srccolname in self.src_cols:
                print(srccolname)
                bucket3 = bucket3.withColumn(srccolname,
                    F.when(col("df1_"+srccolname) != col("df2_"+srccolname), lit(srccolname))
                .otherwise(lit(None)))

            bucket3 = bucket3.withColumn("concated_column",F.concat_ws(",",*self.src_cols)).drop(*self.src_cols)
            bucket3 = bucket3.withColumn("concated_column_output",
                                         F.when(col("concated_column").isNotNull(), concat_ws(" ",col("concated_column"), lit("have not matched")))
                                         .otherwise(lit("All columns have matched"))).drop("concated_column")
            bucket3.show(truncate=False)
            return bucket3

        except Exception as error:
            logging.error(f"Error while processing recon_compare function {error}")
            raise Exception from error
            sys.exit(101)

    def recon_compare(self, srcdf=None, tgtdf=None, key_columns_dict=None):
        """This function compares two dataframes with identical schema and produces output as 4 buckets
        Bucket 1: Key in Left DF (X) unmatched with the key in Right DF.
        Bucket 2: Key matched and values are identical (inner join).
        Bucket 3: key matched (inner join) but values unmatched.
        Bucket 4: Key in Right DF(Y) unmatched with the key in Left DF."""
        try:
            src_df = srcdf if srcdf else self.srcdf
            tgt_df = tgtdf if tgtdf else self.tgtdf

            src_keylist = []
            tgt_keylist = []
            for x in self.key_columns_dict.split(","):
                a = "df1_" + x if x else "df1_" + self.key_columns_dict
                src_keylist.append(a)
                b = "df2_" + x if x else "df2_" + self.key_columns_dict
                tgt_keylist.append(b)

            source_key_columns = ','.join(src_keylist)
            tgt_key_columns = ','.join(tgt_keylist)

            # Create joining columns
            logging.info("Started creating joining condition")
            prop_dict = {"source_join_cols": source_key_columns,
                         "target_join_cols": tgt_key_columns}
            source_join_cols = prop_dict["source_join_cols"]
            target_join_cols = prop_dict["target_join_cols"]

            source_side_join_cols_arr = list(zip(source_join_cols.split(","), target_join_cols.split(",")))
            print(source_side_join_cols_arr)

            for srccolname in src_df.columns:
                src_df = src_df.withColumnRenamed(srccolname, "df1_" + srccolname)

            for tgtcolname in tgt_df.columns:
                tgt_df = tgt_df.withColumnRenamed(tgtcolname, "df2_" + tgtcolname)

            hu_obj = hu.HeliosUtils()
            join_condition = hu_obj.createSafeJoinKey(src_df, tgt_df, source_side_join_cols_arr)
            logging.info("Completed creating joining condition")

            logging.info("Started Bucket1 join")
            bucket1 = src_df.join(tgt_df, join_condition, 'leftanti')

            for colname in self.rename_tgt_cols:
                bucket1 = bucket1.withColumn(colname, lit(None))

            bucket1 = bucket1.withColumn("bucket_name", lit("bucket1"))
            logging.info("Completed Bucket1 join")

            logging.info("Started Bucket4 join")
            bucket4 = tgt_df.join(src_df, join_condition, 'leftanti')
            for colname in self.rename_src_cols:
                bucket4 = bucket4.withColumn(colname, lit(None))

            bucket4 = bucket4.select(*self.rename_src_cols, *self.rename_tgt_cols).withColumn("bucket_name",
                                                                                              lit("bucket4"))
            logging.info("Completed Bucket4 join")

            logging.info("Started Bucket2 and Bucket3 join")
            src_hash_df = src_df.withColumn("src_hash_id",
                                            md5(concat_ws(",", *self.rename_src_cols)))
            tgt_hash_df = tgt_df.withColumn("tgt_hash_id",
                                            md5(concat_ws(",", *self.rename_tgt_cols)))

            cols_to_drop = ['src_hash_id', 'tgt_hash_id']
            bucket12 = src_hash_df.join(tgt_hash_df, join_condition, 'inner')
            bucket2 = bucket12.filter((src_hash_df.src_hash_id == tgt_hash_df.tgt_hash_id)).drop(
                *cols_to_drop).withColumn("bucket_name", lit("bucket2"))

            bucket3 = bucket12.filter((src_hash_df.src_hash_id != tgt_hash_df.tgt_hash_id)).drop(
                *cols_to_drop).withColumn("bucket_name", lit("bucket3"))

            bucket3 = recon_compare31(bucket3)

            logging.info("Completed Bucket2 and Bucket3 join")

            bucket_un1 = bucket1.union(bucket2)
            bucket_un2 = bucket_un1.union(bucket3)
            bucket_union = bucket_un2.union(bucket4)
            return bucket_union

        except Exception as error:
            logging.error(f"Error while processing recon_compare function {error}")
            raise Exception from error
            sys.exit(101)

if __name__ == '__main__':
    spark = SparkSession.builder.appName("df_compare").master("local[*]").getOrCreate()
    # define the schema of the DataFrame
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("firstname", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("city", StringType(), True)
    ])

    # create a list of tuples with the data
    data = [(1, "John", "Doe", "New York"),
            (2, "Jane", "Smith", "San Francisco")]

    # create a DataFrame from the data and the schema
    source_df = spark.createDataFrame(data, schema)

    # create a list of tuples with the data
    data = [(1, "John", "Doe", "New York"),
            (2, "Jane", "Smith1", "San Francisco1")]

    # create a DataFrame from the data and the schema
    target_df = spark.createDataFrame(data, schema)

    key_column_list = "id"

    comparator = ReconComparator(spark, source_df, target_df, key_column_list)
    comparator.recon_compare3(source_df, target_df, key_column_list)