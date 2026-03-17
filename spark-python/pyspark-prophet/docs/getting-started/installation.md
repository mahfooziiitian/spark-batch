# Installation

## Prerequisites

| Requirement | Minimum version |
|---|---|
| Python | 3.11 |
| Java (JDK) | 11 or 17 (required by PySpark) |
| Poetry | 1.8+ |

---

## 1. Clone the repository

```bash
git clone https://github.com/mahfooz_iiitian/pyspark-prophet.git
cd pyspark-prophet
```

---

## 2. Install dependencies with Poetry

```bash
poetry install
```

This installs all dependencies declared in `pyproject.toml`:

```toml
[tool.poetry.dependencies]
python   = "^3.11"
pyspark  = "^3.5.1"
prophet  = "^1.1.5"
numpy    = "1.26.4"      # pinned — Prophet Stan backend requires NumPy < 2.0
pandas   = "<3.0.0"
pyarrow  = "^23.0.1"
plotly   = "^5.22.0"
```

---

## 3. Activate the virtual environment

```bash
poetry shell
```

---

## 4. Verify the installation

```bash
python - <<'EOF'
from prophet import Prophet
from pyspark.sql import SparkSession
import numpy as np
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
print("PySpark:", spark.version)
print("NumPy:  ", np.__version__)
print("Prophet: OK")
spark.stop()
EOF
```

Expected output:
```
PySpark: 3.5.x
NumPy:   1.26.4
Prophet: OK
```

---

## 5. (Optional) Build the MkDocs site

```bash
pip install mkdocs-material mkdocs-minify-plugin
mkdocs serve        # live-reload dev server → http://127.0.0.1:8000
mkdocs build        # build static site → site/
```

---

## Troubleshooting

!!! danger "`ImportError: cannot import name 'bool' from 'numpy'`"
    NumPy 2.x is incompatible with Prophet's Stan backend. Pin to `1.26.4`:
    ```bash
    pip install "numpy==1.26.4"
    ```

!!! warning "`JAVA_HOME not set`"
    PySpark requires Java on `PATH`. Set `JAVA_HOME`:
    ```bash
    export JAVA_HOME=$(/usr/libexec/java_home -v 11)  # macOS
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk      # Linux
    ```

!!! tip "Arrow serialisation errors"
    Ensure `pyarrow` is installed and Arrow is enabled in SparkSession:
    ```python
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    ```
