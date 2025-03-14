"""
Once the logical plan is optimized, the Catalyst will generate one or more physical plans
using the physical operators that match the Spark execution engine.


In addition to the optimizations performed in the logical plan phase, the physical plan phase performs
its own ruled-based optimizations, including combining projections and filtering
into a single operation as well as pushing the projections or filtering predicates all
the way down to the data sources that support this feature, i.e., Parquet.

Again, these optimizations follow the data pruning principle.

The final step the Catalyst performs is to generate the Java bytecode of the cheapest physical plan.

"""
import os

from pyspark.sql import SparkSession, functions

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').appName('Catalyst').getOrCreate()

    data_path = f"{os.environ['DATA_HOME']}/FileData/Parquet/Movies/movies.parquet"

    movies_df = spark.read.load(data_path)

    # perform two filtering conditions
    new_movies_df = (movies_df.filter(functions.expr("produced_year > 1970"))
                     .withColumn("produced_decade",
                                 functions.expr("produced_year + produced_year % 10"))
                     .select("movie_title",
                             "produced_decade").where
                     (functions.expr("produced_decade > 2010"))
                     )

    # display the logical and physical plans
    """
    If you carefully analyze the optimized logical plan, you will see that it combines
    the both filtering conditions into a single filter.
    The physical plan shows that Catalyst both pushes down the filtering of produced_year and performs the projection 
    pruning to the FileScan step.
    """
    new_movies_df.explain(extended=True)
