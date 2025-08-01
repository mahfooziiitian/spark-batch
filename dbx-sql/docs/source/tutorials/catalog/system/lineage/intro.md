# Introduction

Databricks captures lineage at the table and column level via Unity Catalog.

## Lineage system tables

Table name                        | Description
----------------------------------|--------------------------------------------------
system.access.audit               | Includes lineage-like info from audit logs.
system.tables                     | Metadata about tables (indirectly helps lineage).
