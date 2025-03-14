# Identifying shuffles in a Spark query plan

    val jump2Numbers = spark.range(0, 100000,2)
    val jump5Numbers = spark.range(0, 200000, 5)
    val ds1 = jump2Numbers.repartition(3)
    val ds2 = jump5Numbers.repartition(5)
    val joined = ds1.join(ds2) 
    joined.explain

