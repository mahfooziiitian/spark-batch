"""Early environment setup — import before pyspark.pandas.

This module sets environment variables that must be configured before
any ``pyspark.pandas`` import or ``SparkContext`` creation.  Import it
as the first ``spp`` import in any module::

    import spp._env  # noqa: F401  (side-effect import)
    import pyspark.pandas as ps
"""

import os

os.environ.setdefault("PYARROW_IGNORE_TIMEZONE", "1")

if os.environ.get("JAVA_HOME_11"):
    os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
