# Spark Application Structure

A Spark application consists of the following components:

## 1. Driver Program

The central coordinator that runs the main function and schedules tasks.

## 2. Cluster Manager

Handles resource allocation (e.g., YARN, Mesos, Kubernetes, or Spark’s Standalone Cluster Manager).

## 3. Executors

Worker processes that execute tasks and store data in memory or disk.

## 4. Tasks

The smallest unit of execution, running within an executor.
