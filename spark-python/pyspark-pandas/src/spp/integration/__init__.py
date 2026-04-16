"""PySpark–Pandas integration patterns.

Demonstrates the core integration patterns between pandas and PySpark 3.x:

- ``conversion_patterns``        — Spark ↔ pandas ↔ pandas-on-Spark conversions
- ``feature_engineering``        — Feature engineering pipeline (Spark → pandas)
- ``ml_pipeline``                — ML pipeline with Spark prep and pandas training
- ``hybrid_workflow``            — Batch ETL + sampling + debugging patterns
- ``pandas_function_apis``       — mapInPandas, applyInPandas, cogroup APIs
- ``group_feature_engineering``  — Rolling / lag / cumsum per group
- ``ml_preprocessing``           — Per-group fillna / outlier removal / scaling
"""
