# Introduction

workspaces system table to monitor workspaces in your Databricks account. Each row in the table represents the latest known state of an active workspace in your account, including metadata and lifecycle status.

This table is most useful when joined with other system tables. You can use it to get aggregate statistics on reliability, performance, and cost across workspaces in your account.

Table path: This table is located at system.access.workspaces_latest
