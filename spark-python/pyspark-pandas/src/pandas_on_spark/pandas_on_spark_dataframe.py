import os

import pyspark.pandas as ps
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\mahfo\\.virtualenvs\\PysparkPandas-bh8Y5n2t\\Scripts\\python.exe"

if __name__ == '__main__':
    ps.set_option('compute.ops_on_diff_frames', True)
    ps_df1 = ps.range(5)
    ps_df2 = ps.DataFrame({'id': [5, 4, 3]})
    print((ps_df1 - ps_df2).sort_index())
    ps.reset_option('compute.ops_on_diff_frames')
    
