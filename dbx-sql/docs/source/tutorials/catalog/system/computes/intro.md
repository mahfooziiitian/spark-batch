# Introduction

You can use these tables to monitor the activity and metrics of non-serverless all-purpose compute, jobs compute, and Lakeflow Declarative Pipelines compute in your account.

The compute tables include:

1. clusters: Records compute configurations in your account.
2. node_types: Includes a single record for each of the currently available node types, including hardware information.
3. node_timeline: Includes minute-by-minute records of your compute's utilization metrics.

## Node types table schema

The node type table captures the currently available node types with their basic hardware information.

Table path: This system table is located at system.compute.node_types.

## Node timeline table schema

The node timeline table captures node-level resource utilization data at minute granularity. Each record contains data for a given minute of time per instance. This table captures node timelines for the all-purpose compute, jobs compute, Lakeflow Declarative Pipelines compute, and pipeline maintenance compute resources in your account.

Table path: This system table is located at system.compute.node_timeline.
