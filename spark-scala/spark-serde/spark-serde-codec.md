
SparkSession and QueryExecution are transient attributes of a Dataset and therefore do not participate in Dataset serialization.
The only firmly-tied feature of a Dataset is the Encoder.
