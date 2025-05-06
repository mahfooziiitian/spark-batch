import os
import sys
from pathlib import Path

import pandas as pd
import requests
import yaml
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable
# Initialize Spark
spark = SparkSession.builder.appName("REST_API_Ingestion").getOrCreate()

# Load YAML ds
config_path = Path(__file__).parents[0] / "ds.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

api_config = config["dataSources"]["myApiSource"]

# Build API request
headers = api_config.get("headers", {})
params = api_config.get("queryParams", {})
endpoint = api_config["endpoint"]
method = api_config["method"].upper()

# Fetch data (simple version, no pagination yet)
if method == "GET":
    response = requests.get(endpoint, headers=headers, params=params)
else:
    raise ValueError(f"Unsupported method: {method}")

# Check response
response.raise_for_status()


data = response.json()  # Assuming JSON response

print(f"Fetched {len(data)} records from API.")
print(f"Response: {data}")

# Convert to DataFrame
pdf = pd.DataFrame(data)
print(f"{pdf}")
df = spark.createDataFrame(pdf)

# Now df is a Spark DataFrame, ready for processing
df.show(truncate=False, n=100)
