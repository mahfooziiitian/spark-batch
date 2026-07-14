# Examples

Two PySpark scripts demonstrate progressively advanced MongoDB integration
patterns.

## Overview

| Script | What it covers |
| ------ | -------------- |
| [`mongodb_collection.py`](collections.md) | Write, read, filter, write filtered results |
| [`mongodb_aggregations.py`](aggregations.md) | GroupBy, window functions, running totals, rankings |

## Data Flow

```mermaid
graph TD
    subgraph mongodb_collection.py
        A[Create DataFrame] --> B[Write to 'people']
        B --> C[Read from 'people']
        C --> D[Filter age > 100]
        D --> E[Write to 'elders']
    end

    subgraph mongodb_aggregations.py
        F[Create sales DataFrame] --> G[Write to 'sales']
        G --> H[Read from 'sales']
        H --> I[Aggregate by region]
        I --> J[Write to 'region_summary']
        H --> K[Running totals by month]
        K --> L[Write to 'monthly_running_totals']
        I --> M[Rank regions]
        M --> N[Write to 'region_rankings']
    end
```

## Running

```bash
# Collections example
uv run python src/mongondb/mongodb_collection.py

# Aggregations example
uv run python src/mongondb/mongodb_aggregations.py
```

!!! tip "Start MongoDB first"
    Both scripts require a running MongoDB instance. See the
    [infrastructure guide](../infrastructure/index.md) for setup.
