"""
Note that Structured Streaming does not materialize the entire table.
It reads the latest available data from the streaming data source, processes it incrementally to update the result,
and then discards the source data.
It only keeps around the minimal intermediate state data as required to update the result.
"""
if __name__ == '__main__':
    print()