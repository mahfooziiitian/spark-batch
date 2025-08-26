# 🌐 Full Outer Join

A **Full Outer Join** returns **all rows** from both tables:

- All rows from the **left** table
- All rows from the **right** table
- Matches where possible, and `NULL`s where there is no match

---

## 📋 Example

```sql
SELECT
  *
FROM
  employees
FULL OUTER JOIN
  departments
ON
  employees.dept_no = departments.id;
```

---

## 📝 Result Explained

- **All employees** are included, even if they don't belong to a department.
- **All departments** are included, even if they have no employees.
- Non-matching columns are filled with `NULL`.

---

## 🖼️ Visual Representation

| employees.dept_no | employees.name | departments.id | departments.name |
|:-----------------:|:--------------|:--------------:|:----------------|
| 1                 | Alice         | 1              | HR              |
| 2                 | Bob           | NULL           | NULL            |
| NULL              | NULL          | 3              | IT              |

---

## 💡 Real-World Use Cases

- **Customers vs Orders:** Find customers with and without orders, and orders without matching customers.
- **Employees vs Departments:** List all employees and departments, even if some are unassigned or empty.
- **System Logs:** Merge logs from two sources and see all entries, including those without a match.

---

## ⚡ Performance Notes

- A full outer join **shuffles both datasets** (like a sort-merge join).
- Can be **expensive** for very large datasets.
- If one dataset is small, you can **broadcast** it, but Spark **does not support broadcast** with full outer join (only inner, left, right joins).

---

## 🎯 Diagram

```{mermaid}
flowchart TD
    A([Table A]):::left
    B([Table B]):::right
    R([Result = A ∪ B]):::result

    A --> R
    B --> R

classDef left fill:#a2d2ff,stroke:#000,stroke-width:1px;
classDef right fill:#ffc8dd,stroke:#000,stroke-width:1px;
classDef result fill:#caffbf,stroke:#000,stroke-width:2px;
```

**Full Outer Join Result = All of A + All of B (matches + non-matches).**

---
