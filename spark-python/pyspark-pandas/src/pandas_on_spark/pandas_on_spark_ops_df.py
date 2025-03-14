import os

import pyspark.pandas as ps
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\mahfo\\.virtualenvs\\PysparkPandas-bh8Y5n2t\\Scripts\\python.exe"
if __name__ == '__main__':
    ps.set_option('compute.ops_on_diff_frames', True)
    ps_df = ps.range(5)
    ps_ser_a = ps.Series([1, 2, 3, 4])
    # 'ps_ser_a' is not from 'ps_df' DataFrame. So it is considered as a Series not from 'ps_df'.
    ps_df['new_col'] = ps_ser_a
    print(ps_df)
