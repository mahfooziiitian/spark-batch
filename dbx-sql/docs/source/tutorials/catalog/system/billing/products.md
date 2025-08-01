# Products

A product in Databricks refers to a specific feature set or service offered by the platform to support a data engineering, analytics, or AI/ML workload.

## Examples:

Product Name            | Purpose
------------------------|-----------------------------------------------
Databricks SQL          | Run SQL analytics on Delta Lake data
Delta Live Tables (DLT) | Declarative data pipelines
Jobs                    | Schedule and run production workflows
All-Purpose Clusters    | Interactive notebooks and exploratory analysis
Model Serving           | Deploy ML models with real-time APIs
Unity Catalog           | Centralized data governance and access control
Lakehouse Federation    | Query data across multiple cloud data sources
Vector Search           | Native vector DB for retrieval-augmented tasks

##  How to Use This Information

1. Identify your `cloud provider` (AWS, Azure, GCP).
2. `Select workload type`: development (All‑Purpose), production ETLs (Jobs), SQL analytics, DLT pipelines, model serving, etc.
3. `Choose tier`: Standard, Premium, Enterprise—and decide whether to use Photon or serverless variants.
4. `Estimate DBU usage`: Based on workload scale, average DBU consumption per hour.
5. `Add cloud infrastructure costs`: VM hourly rate × hours, plus storage and network charges.
6. `Consider committed‑use discounts`: Private offers can lock in lower DBU rates and cross‑SKU discounts on multi-year contracts 

## Billing origin product reference

Some Databricks products are billed under the same shared SKU.

For example, `Lakehouse Monitoring`, `predictive optimization`, and `serverless workflows` are all billed under the same serverless jobs SKU.

## Product features reference

The `product_features` column is an object containing information about the specific product features used and includes the following key/value pairs:

1. `jobs_tier`: values include LIGHT, CLASSIC, or null
2. `sql_tier`: values include CLASSIC, PRO, or null
3. `dlt_tier`: values include CORE, PRO, ADVANCED, or null
4. `is_serverless`: values include true or false, or null
5. `is_photon`: values include true or false, or null
6. `serving_type`: values include MODEL, GPU_MODEL, FOUNDATION_MODEL, FEATURE, or null
7. `offering_type`: values include BATCH_INFERENCE or null.
8. `networking.connectivity_type`: values include PUBLIC_IP and PRIVATE_IP
