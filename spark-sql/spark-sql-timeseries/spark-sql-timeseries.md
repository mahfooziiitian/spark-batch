# spark sql time series

## Loading data

    CREATE OR REPLACE TEMPORARY VIEW time_series_v1
      USING csv
      OPTIONS (
          path 'file:/mnt/d/Data/TimeSeries/synthetic_timeseries.csv',
          header 'true',
          inferSchema 'true'
      );

## Preparing Data

    with cast_timestamp_data as (
        select *, cast(timestamp as timestamp) as ts 
        from time_series_v1
        order by cast(timestamp as timestamp)
    ),
    last_value_data as (
        select 
            *, 
            last(value, true) over(order by ts rows between unbounded preceding and current row ) as last_value
        from cast_timestamp_data 
    )
    SELECT
        window(ts, '1 month') as window_interval,
        AVG(value) as monthly_avg
    FROM
        last_value_data
    GROUP BY
        window(ts, '1 month')

    create or replace temporary view time_series_order_v1 as
    select *, cast(timestamp as timestamp) as ts from time_series_v1
    order by timestamp

## Handling Missing Values

    select *, last(value, true) over(order by ts rows between unbounded preceding and current row ) as last_value
    from time_series_order_v1;

## Re-sampling

    SELECT
        window(ts, '1 month') as window_interval,
        AVG(value) as monthly_avg
    FROM
        time_series_order_v1
    GROUP BY
        window(ts, '1 month')

## Reference

1. <https://medium.com/@suffyan.asad1/spark-leveraging-window-functions-for-time-series-analysis-in-pyspark-03aa735f1bdf>
