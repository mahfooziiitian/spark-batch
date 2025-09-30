from pyspark.sql import SparkSession
from pyspark.sql.connector import Identifier, Table, TableCatalog
from pyspark.sql.connector.catalog import SupportsNamespaces
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


class MyCustomTable(Table):
    def __init__(self, name, schema):
        self._name = name
        self._schema = schema

    def name(self):
        return self._name

    def schema(self):
        return self._schema

    def capabilities(self):
        # Example: support batch read
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
        else:
            raise Exception(f"Table {key} not found.")

    def name(self):
        return "custom"

    def listNamespaces(self):
        # Return all namespaces
        return [[ns] for ns in self.namespaces]

    def namespaceExists(self, namespace):
        return ".".join(namespace) in self.namespaces

    def createNamespace(self, namespace, metadata=None):
        ns = ".".join(namespace)
        self.namespaces.add(ns)

    def dropNamespace(self, namespace):
        ns = ".".join(namespace)
        self.namespaces.discard(ns)


# Register custom catalog
spark = (
    SparkSession.builder.config(
        "spark.sql.catalog.custom", "metastore.custom.custom_catalog.MyCustomCatalog"
    )
    .config("spark.sql.catalog.custom.myparam", "value")
    .getOrCreate()
)
