"""Partition discovery does occur when subdirectories that are named /key=value/ are present and listing will
automatically recurse into these directories. If these columns appear in the user-provided schema, they will be
filled in by Spark based on the path of the file being read.

The directories that make up the partitioning scheme must be present when the query starts and must remain static.

For example, it is okay to add /data/year=2016/ when /data/year=2015/ was present, but it is invalid to change the
partitioning column (i.e. by creating the directory /data/date=2016-04-17/).
"""