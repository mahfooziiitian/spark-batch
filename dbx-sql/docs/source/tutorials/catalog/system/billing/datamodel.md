# Datamodel

Every billing record includes columns that attribute the usage amount to the specific resources, identities, and products involved.

1. The `usage_metadata` column includes a struct with information about the `resources or objects` involved in the usage.
2. The `identity_metadata` column includes information about the `user or service principal` that incurred the usage.
3. The `custom_tags` column includes tags applied to the compute resource associated with the usage. This also includes tags added by `serverless budget policies` so you can attribute serverless usage.
4. The `billing_origin_product` and `product_features` columns give you information about `the exact product and features` used.

## Usage metadata reference

The values in usage_metadata are all strings that tell you about the workspace objects and resources involved in the usage record.

Only a subset of these values is populated in any given usage record, depending on the compute type and features used.

## Identity metadata reference

The identity_metadata column provides more information about the identities involved in the usage.

The run_as field logs who ran the workload. This values is only populated for certain workload types listed in the table below.
The owned_by field only applies to SQL warehouse usage and logs the user or service principal who owns the SQL warehouse responsible for the usage.
The identity_metadata.created_by field applies to Databricks Apps and logs the email of the user who created the app.
