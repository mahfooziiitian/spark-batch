from delta import DeltaTable


def create_delta_table_with_deletion_vectors(spark):
    """
    Creates a Delta table with deletion vectors enabled.
    """
    print("Creating a Delta table with deletion vectors enabled...")
    data = [
        (1, "Alice", 30),
        (2, "Bob", 24),
        (3, "Charlie", 35),
        (4, "David", 29),
        (5, "Eve", 40),
    ]
    columns = ["id", "name", "age"]
    df = spark.createDataFrame(data, columns)

    # Define the path for the Delta table
    delta_table_path = "/tmp/delta_table_with_deletion_vectors"

    # Write the DataFrame as a Delta table with deletion vectors enabled
    df.write.format("delta").option("delta.enableDeletionVectors", "true").mode(
        "overwrite"
    ).save(delta_table_path)

    print(f"Delta table created at: {delta_table_path}")
    return delta_table_path


def perform_deletes_with_deletion_vectors(spark, delta_table_path):
    """
    Performs delete operations on a Delta table with deletion vectors.
    """
    print("\nPerforming delete operations with deletion vectors...")
    delta_table = DeltaTable.forPath(spark, delta_table_path)

    print("Original table content:")
    delta_table.toDF().show()

    # Delete a row
    print("Deleting row with id = 2...")
    delta_table.delete("id = 2")
    print("Table content after first delete:")
    delta_table.toDF().show()

    # Delete another row
    print("Deleting row with name = 'Charlie'...")
    delta_table.delete("name = 'Charlie'")
    print("Table content after second delete:")
    delta_table.toDF().show()

    # Check the history to see deletion vector operations
    print("\nDelta table history:")
    delta_table.history().show(truncate=False)
    return delta_table
