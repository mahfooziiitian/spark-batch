# Copilot Instructions — pyspark-fhir

This project demonstrates processing **FHIR healthcare data** with PySpark
using the [Bunsen](https://github.com/cerner/bunsen) library for terminology
and code hierarchy management.

## Technology Stack

| Component | Version / Tool |
|-----------|---------------|
| Python | ≥ 3.11 |
| PySpark | 3.5.x |
| Bunsen | FHIR STU3 / R4 |
| Package manager | uv (preferred) |
| Testing | pytest ≥ 8.0 |

## Project Structure

- `main.py` — entry point (stub)
- `fhir-bunsen/` — Bunsen integration code
  - `bunse_spark.py` — loads SNOMED and LOINC hierarchies

## FHIR + Bunsen Patterns

### Imports

```python
from bunsen.stu3.codes import create_hierarchies
from bunsen.codes.loinc import with_loinc_hierarchy
from bunsen.codes.snomed import with_relationships
```

### Creating Hierarchies

```python
hierarchies = create_hierarchies(spark)
```

### Loading SNOMED Relationships

SNOMED relationships are loaded from RF2 Snapshot files:

```python
hierarchies = with_relationships(
    hierarchies,
    "/path/to/snomed/Snapshot/Terminology/sct2_Relationship_Snapshot_INT.txt",
    "http://snomed.info/sct",
    "20230901",
)
```

### Loading LOINC Hierarchy

LOINC hierarchy is loaded from the MULTI-AXIAL CSV file:

```python
hierarchies = with_loinc_hierarchy(
    hierarchies,
    "/path/to/loinc/AccessoryFiles/MultiAxialHierarchy/MultiAxialHierarchy.csv",
    "2.76",
)
```

### Persisting to Database

```python
hierarchies.write_to_database("ontologies")
```

### SparkSession Configuration for Bunsen

Bunsen requires specific JAR packages for FHIR STU3 or R4:

```python
spark = (
    SparkSession.builder
    .appName("FHIRBunsen")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .config("spark.jars.packages", "com.cerner.bunsen:bunsen-spark-stu3:0.5.13")
    .enableHiveSupport()
    .getOrCreate()
)
```

> **Note:** `enableHiveSupport()` is required when writing hierarchies to a
> database (e.g., the `ontologies` database).

## Conventions

- Use `SPARK_MASTER` env var with `local[*]` fallback.
- `from pyspark.sql import functions as F` — never `import *`.
- Always call `spark.stop()` at the end of standalone scripts.
- Keep terminology data paths configurable (env vars or CLI args).

## Things to Avoid

- Do not use `from pyspark.sql.functions import *`.
- Do not omit `spark.stop()` in standalone scripts.
- Do not use `len(df.collect())` — use `df.count()`.
- Do not hard-code paths to SNOMED/LOINC distribution files.
- Do not forget Bunsen JAR packages in SparkSession configuration.
