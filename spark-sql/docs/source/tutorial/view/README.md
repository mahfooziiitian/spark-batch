# View

Views are based on the result-set of an SQL query. `CREATE VIEW` constructs a virtual table that has no physical data therefore other operations like `ALTER VIEW` and `DROP VIEW` only change metadata.

## Key Points in Databricks

1. **Performance:** Views don't store results — they run the underlying query each time.
2. **Materialized Views:** If you need performance, use a Delta table with scheduled refresh instead of a view.
3. **Unity Catalog:** Permanent views can be managed with fine-grained permissions.
4. **Alter/View Changes:** Use `CREATE OR REPLACE VIEW` to update definition without dropping.
