# Sku

A `SKU (Stock Keeping Unit)` in Databricks is a billing unit that defines how you're charged for using a particular product with a specific configuration (e.g., tier, cloud, workload type).

## Examples:

SKU Name                                        | Corresponding Product | Cloud | Tier
------------------------------------------------|-----------------------|-------|-----------
AWS Premium Jobs Compute                        | Jobs Compute          | AWS   | Premium
Azure Enterprise DLT Advanced Compute           | Delta Live Tables     | Azure | Enterprise
GCP Standard All-Purpose Compute (Photon)       | All-Purpose Compute   | GCP   | Standard
AWS Serverless SQL Pro Compute                  | Databricks SQL        | AWS   | Premium
Foundation Model Serving - Llama 3 - 8xA100 GPU | Model Serving         | All   | N/A

Each SKU determines the price per DBU, available features, and security compliance.

`Total cost = Databricks DBUs + Cloud provider compute/storage fees`
