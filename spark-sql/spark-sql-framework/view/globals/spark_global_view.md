# Global view

    val movies=spark.read.json("D:\\data\\movies\\movies.json")

    movies.createOrReplaceGlobalTempView("movies_g")

    spark.sql("select count(*) from global_temp.movies_g").show
  