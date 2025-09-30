# In-Memory Metastore

The in-memory metastore is the default metadata store used by Spark when Hive support is not enabled. All table and schema metadata is stored in memory, making it fast but ephemeral—data is lost when the Spark session terminates.

## Key Features

- **Temporary Storage:** Metadata exists only for the duration of the Spark session.
- **No Persistence:** Data is not saved to disk or any external system.
- **Fast Access:** Suitable for rapid prototyping, testing, or short-lived jobs.

## Use Cases

- Development and testing environments.
- Ephemeral Spark jobs where persistent metadata is not required.
- Scenarios where simplicity and speed are prioritized over durability.

> **Note:** For production workloads or when persistent metadata is needed, consider enabling Hive support or using an external metastore.
