# Schema validation

Delta Lake enforces strict schema validation rules to ensure data quality and prevent inconsistencies when writing data from a DataFrame to a table. These rules determine whether a write operation is compatible with the existing table schema:

*   **Column Existence:** All columns present in the DataFrame must also exist in the target Delta table. If the DataFrame contains columns that are not defined in the table's schema, an `AnalysisException` will be raised, preventing the write operation. Conversely, if the table contains columns not present in the DataFrame, those columns will be populated with `NULL` values for the new records.

*   **Data Type Compatibility:** The data types of columns in the DataFrame must precisely match the corresponding column data types in the target table. Any mismatch in data types will result in an `AnalysisException`, ensuring type safety and preventing data corruption.

*   **Case-Insensitive Column Names:** Delta Lake does not allow column names that differ only by case within the same table (e.g., "Foo" and "foo"). This restriction is in place to prevent potential ambiguities, data corruption, or loss issues that can arise from differences in case sensitivity between Spark (which can operate in case-sensitive or insensitive mode, with insensitive as default) and Parquet (which is case-sensitive when storing and retrieving column information). While Delta Lake is case-preserving, it treats column names as case-insensitive during schema validation to maintain consistency.

These strict validation rules are fundamental to maintaining the integrity and reliability of data stored in Delta Lake tables.

## Schema Evolution

While strict validation is the default, Delta Lake provides powerful features for managing schema changes over time:

*   **Explicit DDL for New Columns:** You can explicitly add new columns to a Delta table using Data Definition Language (DDL) commands. This gives you granular control over schema modifications.

*   **Automatic Schema Evolution:** For scenarios where you want to automatically adapt the table schema to incoming data, Delta Lake supports schema evolution. This feature allows you to automatically add new columns to a table when they appear in a DataFrame being written, simplifying data ingestion pipelines.

These capabilities allow Delta Lake to balance strict data quality enforcement with the flexibility needed for evolving data schemas.
