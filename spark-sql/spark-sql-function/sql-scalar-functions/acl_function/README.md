# ACL

In Spark SQL and Databricks, ACL (Access Control List) isn't a function — it refers to access control mechanisms used to manage permissions on objects like tables, views, notebooks, and clusters.

However, if you're referring to ACLs in terms of SQL operations, Databricks provides SQL-based access control via Unity Catalog or table ACLs, not through a direct acl() function.

## 🔐 ACL in Databricks – Overview

### ✅ 1. Table ACLs (Legacy)

Databricks allows fine-grained access using SQL.

```sql
GRANT SELECT ON TABLE my_table TO `user@example.com`;
REVOKE SELECT ON TABLE my_table FROM `user@example.com`;
```

Enable via workspace admin settings:

1. Enable Table Access Control
2. Assign users/groups to allow SQL access

### ✅ 2. Unity Catalog ACLs (Recommended)

With Unity Catalog, you can manage ACLs at the catalog/schema/table/view/function level using SQL GRANT statements.

```sql
-- Give read access to a user
GRANT SELECT ON TABLE sales_data TO `analyst@example.com`;

-- Give write access to a group
GRANT INSERT, UPDATE ON TABLE orders TO `data-engineers`;

-- Revoke access
REVOKE ALL PRIVILEGES ON TABLE orders FROM `interns`;
```

## 👥 ACL Roles

Common privileges you can grant:

Privilege |Object types
---|---
SELECT| Tables, Views
INSERT| Tables
UPDATE| Tables
DELETE| Tables
USAGE| Catalogs, Schemas, Functions
EXECUTE| Functions
MODIFY| Storage locations (Unity Catalog)
ALL PRIVILEGES| All supported permissions

## 🧪 Example: Managing ACLs in Unity Catalog

```sql
-- Create schema and table
CREATE SCHEMA IF NOT EXISTS finance;
CREATE TABLE IF NOT EXISTS finance.budget (id INT, dept STRING, amount INT);

-- Grant SELECT to analyst
GRANT SELECT ON TABLE finance.budget TO `analyst@datacorp.com`;

-- Revoke all permissions from user
REVOKE ALL PRIVILEGES ON TABLE finance.budget FROM `old_user@datacorp.com`;
```

## 🧠 Check ACLs

You can inspect privileges with:

```sql
SHOW GRANTS ON TABLE finance.budget;
```

Or check for a specific user:

```sql
SHOW GRANTS TO USER `analyst@datacorp.com`;
```

## 🛡️ Notes

1. Unity Catalog is recommended for enterprise-level governance.
2. ACLs apply to SQL, notebooks, data access APIs, etc.
3. ACLs work with groups, service principals, and users.

### 🚫 No acl() Function in Spark SQL

1. If you're asking for a literal acl() function in SQL, no such function exists. ACLs are managed through SQL DDL statements (GRANT, REVOKE, SHOW GRANTS), not functions.
