# Jobs

`lakeflow` system tables, which record job activity in your account. These tables include records from all workspaces in your account deployed in the same cloud region. To see records from another region, you must view the tables from a workspace deployed in that region.

## Requirements
To access these system tables, users must either:

1. Be both a metastore admin and an account admin, or
2. Have USE and SELECT permissions on the system schemas. 

## Tables

### Jobs

The jobs table is a slowly changing dimension table (SCD2). When a row changes, a new row is emitted, logically replacing the previous one.

Table path: system.lakeflow.jobs

### Job task table schema

The job tasks table is a slowly changing dimension table (SCD2). When a row changes, a new row is emitted, logically replacing the previous one.

Table path: system.lakeflow.job_tasks

### Job run timeline table schema

The job run timeline table is immutable and complete at the time it is produced.

Table path: system.lakeflow.job_run_timeline

### Job task run timeline table schema

### Pipelines table schema

The pipelines table is a slowly changing dimension table (SCD2). When a row changes, a new row is emitted, logically replacing the previous one.

Table path: system.lakeflow.pipelines
