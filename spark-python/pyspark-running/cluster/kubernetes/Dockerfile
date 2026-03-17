# Dockerfile for PySpark on Kubernetes
#
# Build:
#   docker build -t my-registry/pyspark-job:latest .
#   docker push my-registry/pyspark-job:latest
#
# The official apache/spark image already includes Python and PySpark.
# Add your application code and any extra dependencies below.

FROM apache/spark:3.5.0-python3

# Install extra Python packages (if needed)
# USER root
# RUN pip install pandas pyarrow boto3
# USER spark

# Copy application code into the work directory
COPY k8s_example.py /opt/spark/work-dir/

# The entrypoint is defined in the base image — spark-submit is called
# by the Kubernetes driver/executor launch scripts automatically.
