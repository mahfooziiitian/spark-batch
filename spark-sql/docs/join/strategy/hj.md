# :material-cog-transfer: Hash Join in Spark

A **hash join** is a highly efficient join strategy in Spark, especially for large datasets. It works by building a hash table from one side of the join (typically the smaller DataFrame) and then streaming the other side to find matching rows.


### :material-sitemap: Overview

```mermaid
graph LR
    S[Small DF] -->|Build| HT[Hash Table]
    L[Large DF] -->|Probe| HT
    HT --> O[Joined Result]
```

---

## :material-puzzle-outline: How Hash Join Works

1. **Build Phase:**  
    Spark constructs a hash table using the *join key* from the smaller DataFrame.
2. **Probe Phase:**  
    The larger DataFrame is scanned, and each row is matched against the hash table.
3. **Output:**  
    Rows with matching keys are joined and emitted as output.

> :material-lightning-bolt: **Note:** Hash Join is only supported for *equality joins* (`=` condition).

---

## :material-office-building-cog:️ Hash Join in Spark: Node-Level Strategy

- Hash join operates **per node** in a Spark cluster.
- Each node joins its local partitions:
  1. Build a hash table from the *small table* using the join key.
  2. Loop over the *large table* and match rows using the hashed join key.

---

## :material-star: Variants of Hash Join in Spark

| Type                  | When Used                                         | Build Side                |
|-----------------------|---------------------------------------------------|---------------------------|
| **Broadcast Hash Join** | When one side is small enough to broadcast        | Broadcast (small) side    |
| **Shuffle Hash Join**   | When both sides are large; Spark shuffles data   | Smaller side (post-shuffle)|

---

## :material-check-circle-outline: Requirements for Hash Join

- **Join condition:** Must be *equality only* (`colA = colB`)
- **Supported join types:** `inner`, `left outer`, `right outer`, `semi`, `anti`

---

## :material-refresh: Execution Flow

1. **Build Phase:**  
    Hash table is built from the smaller DataFrame on the join key.
2. **Probe Phase:**  
    Each row from the larger DataFrame probes the hash table to find matches.
3. **Emit:**  
    Matching rows are output.

---

```mermaid
flowchart TB

subgraph Executor1
     smallPart1[Small DF Partition 1]
     largePart1[Large DF Partition 1]
end

subgraph Executor2
     smallPart2[Small DF Partition 2]
     largePart2[Large DF Partition 2]
end

smallPart1 --> hashTable1[Build Hash Table]
smallPart2 --> hashTable2[Build Hash Table]

largePart1 --> probe1[Probe Hash Table]
largePart2 --> probe2[Probe Hash Table]

hashTable1 --> probe1
hashTable2 --> probe2

probe1 --> out1[Join Output 1]
probe2 --> out2[Join Output 2]
```

---

> :material-lightbulb-outline: **Tip:**  
> Use Broadcast Hash Join when one DataFrame is small enough to fit in memory. For larger datasets, Spark automatically chooses Shuffle Hash Join or other strategies.
