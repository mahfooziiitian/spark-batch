# Skew data

Data is skewed when its distribution curve is asymmetrical (as compared to a normal distribution curve that is perfectly symmetrical) and skewness is the measure of the asymmetry.

The skewness for a normal distribution is 0.

There are 2 different types of skews in data, left(negative) or right(positive) skew.

## Effects of skewed data

Degrades the model's ability (especially regression based models) to describe typical cases as it has to deal with rare cases on extreme values.
ie right skewed data will predict better on data points with lower value as compared to those with higher values.

Skewed data also does not work well with many statistical methods.

However, tree based models are not affected.

## To check for skew in data:

    df.skew().sort_values(ascending=False)

