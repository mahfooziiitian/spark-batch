from pyspark import SparkContext


class FileDataSource:
    def __init__(self, sc: SparkContext, file_path: str):
        self.sc = sc
        self.file_path = file_path

    def read_text(self):
        """Read a text file as an RDD of lines."""
        return self.sc.textFile(self.file_path)

    def read_csv(self, delimiter=","):
        """Read a CSV file as an RDD of lists."""
        rdd = self.sc.textFile(self.file_path)
        return rdd.map(lambda line: line.split(delimiter))

    def save_rdd(self, rdd, output_path):
        """Save an RDD to a text file."""
        rdd.saveAsTextFile(output_path)


# Example usage:
if __name__ == "__main__":
    sc = SparkContext(appName="FileDataSourceExample")
    ds = FileDataSource(sc, "data/input.txt")

    # Read text file
    lines_rdd = ds.read_text()
    print(lines_rdd.take(5))

    # Read CSV file
    csv_rdd = ds.read_csv()
    print(csv_rdd.take(5))

    # Save RDD to file
    ds.save_rdd(lines_rdd, "data/output.txt")

    sc.stop()
