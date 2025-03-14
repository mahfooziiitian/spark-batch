# Cache sql table

    cache table data

## Getting data from cached table

    select * from data

## Cached table

    println(spark.sql("select * from data").queryExecution.withCachedData.numberedTreeString)

## Explain the query after caching

    spark.sql("select id as newId,id from data").explain(extended = true)

## get dag after caching

    println(spark.sql("select id as newId,id from data").queryExecution.withCachedData.numberedTreeString)

## clear cache

    CLEAR CACHE
