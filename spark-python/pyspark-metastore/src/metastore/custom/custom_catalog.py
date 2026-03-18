import os

from pyspark.sql import SparkSession
from pyspark.sql.connector import Identifier, Table, TableCatalog
from pyspark.sql.connector.catalog import SupportsNamespaces
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

CUSTOM_CATALOG_CLASS = os.environ.get(
    "CUSTOM_CATALOG_CLASS", "metastore.custom.custom_catalog.MyCustomCatalog"
)
CUSTOM_CATALOG_PARAM = os.environ.get("CUSTOM_CATALOG_PARAM", "value")


class MyCustomTable(Table):
    def __init__(self, name, schema):
        self._name = name
        self._schema = schema

    def name(self):
        return self._name

    def schema(self):
        return self._schema

    def capabilities(self):
        return {"BATCH_READ"}


class MyCustomCatalog(TableCatalog, SupportsNamespaces):
    def __init__(self):
        self.tables = {
            "default.sample_table": MyCustomTable(
                "sample_table",
                StructType(
                    [
                        StructField("id", StringType(), True),
                        StructField("value", StringType(), True),
                    ]
                ),
            ),
            "default.another_table": MyCustomTable(
                "another_table",
                StructType(
                    [
                        StructField("number", IntegerType(), True),
                        StructField("description", StringType(), True),
                    ]
                ),
            ),
        }
        self.namespaces = {"default"}

    def listTables(self, namespace):
        ns = ".".join(namespace)
        return [
            Identifier.of(ns, name.split(".")[1])
            for name in self.tables
            if name.startswith(ns + ".")
        ]

    def loadTable(self, ident):
        key = ".".join(ident.namespace) + "." + ident.name
        if key in self.tables:
            return self.tables[key]
        raise Exception(f"Table {key} not found.")

    def createTable(self, ident, schema, partitions=None, properties=None):
        key = ".".join(ident.namespace) + "." + ident.name
        if key in self.tables:
            raise Exception(f"Table {key} already exists.")
        ns = ".".join(ident.namespace)
        if ns not in self.namespaces:
            raise Exception(f"Namespace {ns} does not exist.")
        table = MyCustomTable(ident.name, schema)
        self.tables[key] = table
        return table

    def dropTable(self, ident):
        key = ".".join(ident.namespace) + "." + ident.name
        if key not in self.tables:
            raise Exception(f"Table {key} not found.")
        del self.tables[key]
        return True

    def name(self):
        return "custom"

    def listNamespaces(self):
        return [[ns] for ns in self.namespaces]

    def namespaceExists(self, namespace):
        return ".".join(namespace) in self.namespaces

    def createNamespace(self, namespace, metadata=None):
        ns = ".".join(namespace)
        self.namespaces.add(ns)

    def dropNamespace(self, namespace):
        ns = ".".join(namespace)
        self.namespaces.discard(ns)


def create_spark_session():
    return (
        SparkSession.builder.appName("CustomCatalog")
        .config("spark.sql.catalog.custom", CUSTOM_CATALOG_CLASS)
        .config("spark.sql.catalog.custom.myparam", CUSTOM_CATALOG_PARAM)
        .getOrCreate()
    )


def demonstrate_custom_catalog(spark):
    print("=== Custom Catalog Operations ===")

    print("\n-- Namespaces in custom catalog --")
    spark.sql("SHOW NAMESPACES IN custom").show(truncate=False)

    print("\n-- Tables in custom.default --")
    spark.sql("SHOW TABLES IN custom.default").show(truncate=False)

    print("\n-- Describe custom.default.sample_table --")
    spark.sql("DESCRIBE TABLE custom.default.sample_table").show(truncate=False)

    print("\n-- Describe custom.default.another_table --")
    spark.sql("DESCRIBE TABLE custom.default.another_table").show(truncate=False)

    print("\n-- Create a new namespace --")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS custom.analytics")
    spark.sql("SHOW NAMESPACES IN custom").show(truncate=False)

    print("\n-- Drop the new namespace --")
    spark.sql("DROP NAMESPACE IF EXISTS custom.analytics")
    spark.sql("SHOW NAMESPACES IN custom").show(truncate=False)


def demonstrate_catalog_registration(spark):
    print("=== Custom Catalog Registration ===")

    print("\n-- How the custom catalog is registered --")
    print(f"  spark.sql.catalog.custom = {CUSTOM_CATALOG_CLASS}")
    print(f"  spark.sql.catalog.custom.myparam = {CUSTOM_CATALOG_PARAM}")

    print("\n-- Active Spark config for custom catalog --")
    all_conf = spark.sparkContext.getConf().getAll()
    for key, val in sorted(all_conf):
        if "catalog.custom" in key:
            print(f"  {key} = {val}")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        demonstrate_catalog_registration(spark)
        demonstrate_custom_catalog(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
