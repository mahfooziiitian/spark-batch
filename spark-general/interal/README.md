# Internal

## Diagram

```mermaid
graph TD
    A[Driver Program] -->|Sends Tasks| B[Cluster Manager]
    B --> C{Resource Manager}
    C --> D[YARN]
    C --> E[Mesos]
    C --> F[Kubernetes]
    C --> G[Spark Standalone]
    
    A --> H[DAG Scheduler]
    H --> I{Stages}
    I --> J[Task Scheduler]
    J --> K[Executor 1]
    J --> L[Executor 2]
    J --> M[Executor n]
    
    K --> N[Task 1]
    K --> O[Task 2]
    L --> P[Task 3]
    M --> Q[Task n]
    
    subgraph Memory Management
        R[Storage Memory]
        S[Execution Memory]
        T[User Memory]
        U[Reserved Memory]
    end

    A --> V[Spark SQL]
    V --> W[Catalyst Optimizer]
    W --> X[Logical Plan Optimization]
    W --> Y[Physical Plan Optimization]
    
    X --> Z[Predicate Pushdown]
    Y --> AA[Execution Plan Selection]
    
    subgraph Shuffle Mechanism
        AB[Map Phase]
        AC[Reduce Phase]
    end

    A --> AD[Tungsten Engine]
    AD --> AE[Memory Management]
    AD --> AF[Code Generation]
    AD --> AG[Vectorized Execution]
    
    subgraph Fault Tolerance
        AH[Lineage Recovery]
        AI[Checkpointing]
        AJ[Speculative Execution]
    end

    AH --> AK[Recompute Lost Partitions]
    AI --> AL[Store Data in HDFS]
    AJ --> AM[Duplicate Task Execution]
    
    style B fill:#f9f,stroke:#333,stroke-width:4px
    style H fill:#f9f,stroke:#333,stroke-width:4px
    style W fill:#ffccff,stroke:#333,stroke-width:4px
    style AD fill:#b5e7a0,stroke:#333,stroke-width:4px
```

1. Driver Program sends tasks to the Cluster Manager.
2. The Cluster Manager interacts with different resource managers (YARN, Mesos, Kubernetes, Spark Standalone).
3. The DAG Scheduler splits tasks into stages, and the Task Scheduler assigns tasks to Executors.
4. Executors perform tasks, interact with Memory Management, and use Shuffle Mechanism for data exchange.
5. Spark SQL uses Catalyst Optimizer for logical and physical plan optimization.
6. Fault Tolerance mechanisms like lineage recovery, checkpointing, and speculative execution ensure resilience.

Daigram 2

```mermaid
graph TD;
    A[Driver Program] -->|Submits Job| B[DAG Scheduler];
    B -->|Splits into Stages| C[Task Scheduler];
    C -->|Assigns Tasks| D[Executors];
    
    subgraph Cluster Manager
        E[YARN / Mesos / Standalone / Kubernetes]
    end
    
    A -->|Requests Resources| E;
    E -->|Allocates Executors| D;
    
    subgraph Executors
        D1[Task 1] -->|Processes Data| F[Shuffle Read/Write];
        D2[Task 2] -->|Processes Data| F;
        D3[Task 3] -->|Processes Data| F;
    end
    
    F -->|Data Exchange| D1;
    F -->|Data Exchange| D2;
    F -->|Data Exchange| D3;
    
    D -->|Stores Data in Memory/Disk| G[Spark Storage Memory]
```

The driver program submitting jobs.
DAG scheduler breaking jobs into stages and passing tasks to the task scheduler.
The task scheduler assigning tasks to executors, which process data and handle shuffle operations.
The cluster manager allocating resources and launching executors.
Executors storing processed data in memory/disk.
