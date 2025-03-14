# unpivot

    CREATE TABLE IF NOT EXISTS sales_data (
        Category STRING,
        Jan_Sales STRING,
        Feb_Sales STRING,
        Mar_Sales STRING
    );

## Insert data into the table

    INSERT INTO sales_data VALUES ("A", 100, 200, 300);
    INSERT INTO sales_data VALUES ("B", 400, 500, 600);
    INSERT INTO sales_data VALUES ("C", 700, 800, 900);

## unpivoting

    SELECT
        Category,
        Months,
        Sales
    FROM
        sales_data
    LATERAL VIEW
        stack(3, 'Jan', Jan_Sales, 'Feb', Feb_Sales, 'Mar', Mar_Sales) as Months, Sales;
