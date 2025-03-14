import os

import pyspark.pandas as ps

os.environ["PYSPARK_PYTHON"] = "C:\\Users\\mahfo\\.virtualenvs\\PysparkPandas-bh8Y5n2t\\Scripts\\python.exe"

if __name__ == '__main__':
    ps_df = ps.range(10)
    pdf = ps_df.to_pandas()
    print(pdf.values)
    ps.from_pandas(pdf).show()

