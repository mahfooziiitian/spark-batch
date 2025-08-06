-- unpivot
DROP TABLE IF EXISTS sales_data;
CREATE TABLE IF NOT EXISTS sales_data (
    category STRING,
    jan_sales STRING,
    feb_sales STRING,
    mar_sales STRING
);
-- Insert data into the table
INSERT INTO sales_data
VALUES ("A", 100, 200, 300);
INSERT INTO sales_data
VALUES ("B", 400, 500, 600);
INSERT INTO sales_data
VALUES ("C", 700, 800, 900);
-- unpivoting
SELECT
    category,
    months,
    sales
FROM sales_data LATERAL VIEW stack(
    3,
    "Jan",
    jan_sales,
    "Feb",
    feb_sales,
    "Mar",
    mar_sales
) as months,
sales;
DROP TABLE IF EXISTS sales_data;
