"""Pandas UDFs — vectorized user-defined functions.

Modules
-------
- ``pandas_udf``       — Series→Series, aggregate, iterator UDFs
- ``grouped_map_udf``  — applyInPandas (grouped map)
- ``cogroup_udf``      — cogroup().applyInPandas
- ``map_in_pandas``    — mapInPandas (batch-wise transforms)
"""

from spp.pandas_udf.grouped_map_udf import normalize_within_group, subtract_group_mean
from spp.pandas_udf.cogroup_udf import merge_scores
from spp.pandas_udf.map_in_pandas import add_double_age, add_bmi

__all__ = [
    "normalize_within_group",
    "subtract_group_mean",
    "merge_scores",
    "add_double_age",
    "add_bmi",
]
