# Common Table Expressions (CTEs)

Allow for the creation of temporary result sets that can be referenced within a `SELECT`, `INSERT`, `UPDATE`, or `DELETE` statement.

    WITH department_avg AS (
        SELECT
            department,
            AVG(salary) as avg_salary
        FROM
            employees
        GROUP BY
            department
    )
    SELECT
        e.name,
        e.salary,
        d.avg_salary
    FROM
        employees e
    JOIN
        department_avg d
    ON
        e.department = d.department
