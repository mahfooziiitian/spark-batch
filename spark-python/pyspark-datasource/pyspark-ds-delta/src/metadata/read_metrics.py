"""
Restore metrics
RESTORE reports the following metrics as a single row DataFrame once the operation is complete:

1. table_size_after_restore: The size of the table after restoring.

2. num_of_files_after_restore: The number of files in the table after restoring.

3. num_removed_files: Number of files removed (logically deleted) from the table.

4. num_restored_files: Number of files restored due to rolling back.

5. removed_files_size: Total size in bytes of the files that are removed from the table.

6. restored_files_size: Total size in bytes of the files that are restored.

"""
if __name__ == '__main__':
    print('Hello World!')
