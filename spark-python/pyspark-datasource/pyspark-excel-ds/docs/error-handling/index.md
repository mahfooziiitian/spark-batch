# Error Handling

Excel files come from business users, not engineered pipelines — expect
missing files, malformed rows, and inconsistent structure. This section
covers the failure modes `pys_excel` surfaces and how to handle them.

| Scenario | Page |
|----------|------|
| Source file doesn't exist | [Missing File](missing-file.md) |
| Rows with bad/missing/extra values | [Malformed Rows](malformed-rows.md) |

General principles:

- `ExcelReader.read()`/`read_all_sheets()` propagate pandas/openpyxl
  exceptions unchanged (e.g. `FileNotFoundError`) rather than swallowing them
  — catch and handle at the call site for your pipeline's needs.
- Prefer an [explicit schema](../schema/explicit-schema.md) plus
  [`na_values`](../properties/na-values-and-types.md) tuning over
  after-the-fact cleanup, since Excel type-drift is usually a source-data
  problem, not a code problem.
- For scheduled ingestion, wrap `excel_to_table`/`upsert_table_from_excel`
  calls with your own retry/alerting logic — these functions intentionally do
  not swallow errors so failures are visible to orchestration (e.g. a
  Databricks Job).
