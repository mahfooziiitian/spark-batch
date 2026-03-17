"""
PySpark Parallel Queries — reference implementations.

Sub-packages
------------
thread_jobs     : threading.Thread — two or more named parallel actions
threadpool      : ThreadPool fan-out — DataFrame actions, JDBC ingestion,
                  horizontal parallelism
futures         : ThreadPoolExecutor futures — submit-before-get pattern
queue_pool      : Queue-based bounded worker pool
cancellation    : InheritableThread + job-group cancellation
scheduling      : FAIR scheduler, pool assignment, threading_fair demo
utils           : shared SparkSession factory and diagnostics
"""
