# prophet

Prophet follows the sklearn model API.

Prophet is a procedure for forecasting time series data based on an additive model where non-linear trends are fit with yearly, weekly, and daily seasonality, plus holiday effects.

It works best with time series that have strong seasonal effects and several seasons of historical data.

Prophet is robust to missing data and shifts in the trend, and typically handles outliers well.

We create an instance of the `Prophet` class and then call its `fit` and `predict` methods.

The input to Prophet is always a dataframe with two columns:

1. ds
2. y

The `ds (datestamp)` column should be of a format expected by Pandas, ideally `YYYY-MM-DD` for a date or `YYYY-MM-DD HH:MM:SS` for a timestamp.

The `y` column must be `numeric`, and represents the measurement we wish to forecast.

1. <https://www.databricks.com/blog/2020/01/27/time-series-forecasting-prophet-spark.html>
2. <https://www.kaggle.com/code/qingyi/time-series-data-analysis-with-spark>
3. <https://www.kaggle.com/code/prashant111/tutorial-time-series-forecasting-with-prophet>

