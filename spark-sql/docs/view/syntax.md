# :material-code-tags: View Syntax

Views are created with `CREATE VIEW` or `CREATE TEMP VIEW` statements.

### :material-sitemap: Overview

```mermaid
graph LR
    A["CREATE OR REPLACE VIEW"] --> B[View name]
    B --> C["AS SELECT ..."]
    C --> D[View stored in metastore]
    E["CREATE OR REPLACE TEMP VIEW"] --> F[Temp view name]
    F --> G["AS SELECT ..."]
    G --> H[Session-scoped view]
```

---

## 📌 Create View

```sql
CREATE OR REPLACE VIEW my_view AS
SELECT * FROM orders WHERE amount > 100;
```

## 📌 Create Temp View

```sql
CREATE OR REPLACE TEMP VIEW my_temp_view AS
SELECT * FROM orders WHERE amount > 100;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Persist query logic | Use permanent views |
| Session-only logic | Use temp views |
