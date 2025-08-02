# group set

`GROUPING SETS` allows you to define multiple groupings in a single query, providing flexibility to specify exactly which groupings you want.

`GROUPING SETS` allow you to compute `multiple groupings` in a single query, acting like a controlled `GROUP BY CUBE`.

It's used to:

1. Avoid multiple UNION ALL queries
2. Generate subtotal and grand total rows

## Use Case

When you need specific combinations of grouped aggregations.

## Syntax

ANSI SQL style — supported in Spark SQL.

```sql
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
```

This query calculates total sales for each of the specified grouping sets:

1. (date, region, product)
2. (date, region)
3. (region, product)
4. (region),
5. an overall total.

## When to Use What?

Use Case                 | Use
-------------------------|--------------
Subtotals with hierarchy | ROLLUP
All possible subtotals   | CUBE
Custom combinations      | GROUPING SETS

## Flow

```mermaid
flowchart TD
    A[Start: Need Aggregated Reports] --> B{What type of subtotaling?}

    B --> C["Hierarchical Subtotals<br/>(e.g. Region → Country)"]
    C --> D[Use ROLLUP]

    B --> E["All Combinations of Groupings<br/>(e.g. Region, Product)"]
    E --> F[Use CUBE]

    B --> G["Custom Subtotals only<br/>(e.g. Region + Product, Region, Grand Total)"]
    G --> H[Use GROUPING SETS]

    D --> Z["Supports: (a, b), (a), ()"]
    F --> Y["Supports: (a, b), (a), (b), ()"]
    H --> X["Supports: Any groupings like (a, b), (a), (b), (), (a, c)"]

```

##  When to Use

Goal                        | Use GROUPING SETS
----------------------------|------------------
Subtotals                   | ✅
Report summaries            | ✅
Avoid UNION of many queries | ✅
Custom rollups              | ✅
