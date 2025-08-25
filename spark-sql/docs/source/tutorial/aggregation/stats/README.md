# Stats aggregation

## Common Statistical Aggregation Functions in Spark SQL
Function               | Description
-----------------------|------------------------------------------------
AVG(expr)              | Average (mean) of values
SUM(expr)              | Sum of values
MIN(expr)              | Minimum value
MAX(expr)              | Maximum value
COUNT(expr)            | Number of non-null values
VAR_POP(expr)          | Population variance
VAR_SAMP(expr)         | Sample variance
STDDEV_POP(expr)       | Population standard deviation
STDDEV_SAMP(expr)      | Sample standard deviation
SKEWNESS(expr)         | Skewness of the expression
KURTOSIS(expr)         | Kurtosis of the expression
COVAR_POP(x, y)        | Population covariance between x and y
COVAR_SAMP(x, y)       | Sample covariance between x and y
CORR(x, y)             | Pearson correlation coefficient between x & y
PERCENTILE(col, array) | Approximate percentile(s) (Databricks/Spark 3+)
