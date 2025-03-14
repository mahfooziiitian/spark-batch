# 

https://www.projectpro.io/recipes/implementation-of-scd-slowly-changing-dimensions-type-2-spark-sql

// get first state by date for each pk group
val w = Window.partitionBy($"pk").orderBy($"startDate")
val updates = df_new.withColumn("rn", row_number.over(w)).filter("rn = 1").select($"pk", $"startDate")

// join with old data and update old values when there is match
val joinOldNew = df_old.join(updates.alias("new"), Seq("pk"), "left")
                       .withColumn("endDate", when($"endDate".isNull && $"active" === lit(1) && $"new.startDate".isNotNull, $"new.startDate").otherwise($"endDate"))
                       .withColumn("active", when($"endDate".isNull && $"active" === lit(1) && $"new.startDate".isNotNull, lit(0)).otherwise($"active"))
                       .drop($"new.startDate")

// union all
val result = joinOldNew.unionAll(df_new) 

https://priteshjo.medium.com/working-with-scd-type-2-in-pyspark-8eb32963724a
https://stackoverflow.com/questions/75261113/implement-scd-type-2
https://stackoverflow.com/questions/62001386/create-scd2-table-from-sourcefile-that-contains-multiple-updates-for-one-id-usin
