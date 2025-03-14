import os

import pyspark.pandas as ps
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\mahfo\\.virtualenvs\\PysparkPandas-bh8Y5n2t\\Scripts\\python.exe"
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

if __name__ == '__main__':
    print('{:-^60}'.format('Default value'))
    print(ps.options.display.max_rows)

    print('{:-^60}'.format('After equality = '))
    ps.options.display.max_rows = 10
    print(ps.options.display.max_rows)

    print('{:-^60}'.format('After set_option'))
    ps.set_option("display.max_rows", 101)
    print(ps.get_option("display.max_rows"))

    print('{:-^60}'.format('After reset'))
    ps.reset_option("display.max_rows")
    print(ps.get_option("display.max_rows"))

    print('{:-^60}'.format('option_context'))
    with ps.option_context("display.max_rows", 10, "compute.max_rows", 5):
        print(ps.get_option("display.max_rows"))
        print(ps.get_option("compute.max_rows"))
