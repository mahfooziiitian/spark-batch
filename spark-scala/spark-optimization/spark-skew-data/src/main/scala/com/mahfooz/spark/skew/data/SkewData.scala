package com.mahfooz.spark.skew.data

object SkewData {

  def main(args: Array[String]): Unit = {

    /*
    # Adding random values to one side of the join
    df_big = df_big.withColumn('city', F.concat(df['city'], F.lit('_'), F.lit(F.floor(F.rand(seed=17) * 5) + 1)))
    # Exploding corresponding values in other table to match the new values of initial table
      df_medium = df_medium.withColumn('city_exploded', F.explode(F.array([F.lit(i) for i in range(1,6)])))
    df_medium = df_medium.withColumn('city_exploded', F.concat(df_medium['city'], F.lit('_'), df_medium['city_exploded'])). \
    drop('city').withColumnRenamed('city_exploded', 'city')
    # joining
    df_join = df_big.join(df_medium, on=['city'], how='inner')


    val partitionNumber = df.rdd.getNumPartitions()
    val dataDistribution = df.rdd.glom().map(length).collect()

     */
  }
}
