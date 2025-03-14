# group set

GROUPING SETS allows you to define multiple groupings in a single query, providing flexibility to specify exactly which groupings you want.

## Use Case

When you need specific combinations of grouped aggregations.

## examples

    SELECT
        date,
        region,
        product,
        SUM(amount) AS total_sales
    FROM
        sales_dt
    GROUP BY
        GROUPING SETS (
            (date, region, product),
            (date, region),
            (region, product),
            (region),
            ()
        )

## explanation

This query calculates total sales for each of the specified grouping sets:

1. (date, region, product)
2. (date, region)
3. (region, product)
4. (region),
5. an overall total.
