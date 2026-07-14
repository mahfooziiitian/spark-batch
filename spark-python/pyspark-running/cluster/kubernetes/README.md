# PySpark on Kubernetes

Kubernetes (K8s) is the modern, cloud-native way to run Spark. Spark 2.3+
includes a native Kubernetes scheduler. No Hadoop required.

## Architecture

```
┌─────────────────────────────────────────────────┐
│  Kubernetes Cluster                              │
│                                                 │
│  ┌───────────────┐   spawns   ┌───────────────┐ │
│  │  Driver Pod   │ ─────────► │ Executor Pod  │ │
│  │  (spark-submit│            │               │ │
│  │   --deploy    │            │               │ │
│  │    cluster)   │            └───────────────┘ │
│  └───────────────┘                              │
└─────────────────────────────────────────────────┘
```

## Prerequisites

1. A running Kubernetes cluster (minikube, EKS, GKE, AKS, etc.)
2. `kubectl` configured (`~/.kube/config`)
3. A Docker image with your PySpark code

## Build the Docker image

```dockerfile
# Dockerfile  (see Dockerfile in this folder)
FROM apache/spark:3.5.0-python3
COPY k8s_example.py /opt/spark/work-dir/
```

```bash
docker build -t my-registry/pyspark-job:latest .
docker push my-registry/pyspark-job:latest
```

## Create a service account

```bash
kubectl create serviceaccount spark
kubectl create clusterrolebinding spark-role \
  --clusterrole=edit \
  --serviceaccount=default:spark \
  --namespace=default
```

## Submit the job

### Cluster mode (driver runs in a K8s pod)

```bash
spark-submit \
  --master k8s://https://$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}') \
  --deploy-mode cluster \
  --name pyspark-k8s-job \
  --conf spark.kubernetes.container.image=my-registry/pyspark-job:latest \
  --conf spark.kubernetes.namespace=default \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.executor.instances=3 \
  --conf spark.executor.memory=2g \
  --conf spark.executor.cores=1 \
  local:///opt/spark/work-dir/k8s_example.py
```

> Note: `local://` is a path **inside the container**, not your local machine.

### Minikube quick start

```bash
minikube start --cpus 4 --memory 8192
eval $(minikube docker-env)          # point Docker to minikube's daemon
docker build -t pyspark-job:latest .

spark-submit \
  --master k8s://https://$(minikube ip):8443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=pyspark-job:latest \
  --conf spark.kubernetes.container.image.pullPolicy=Never \
  --conf spark.executor.instances=2 \
  local:///opt/spark/work-dir/k8s_example.py
```

## Monitor pods

```bash
kubectl get pods                           # watch driver + executor pods
kubectl logs <driver-pod-name>             # stream driver logs
kubectl describe pod <driver-pod-name>     # debug scheduling issues
```

## Common K8s configs

| Config | Description |
|--------|-------------|
| `spark.kubernetes.container.image` | Docker image for driver and executors |
| `spark.kubernetes.namespace` | K8s namespace |
| `spark.kubernetes.executor.request.cores` | CPU request per executor |
| `spark.kubernetes.executor.limit.cores` | CPU limit per executor |
| `spark.kubernetes.node.selector.<label>` | Pin pods to specific nodes |
| `spark.kubernetes.executor.volumes.*` | Mount PVCs / ConfigMaps |

## Local test

```bash
spark-submit --master local[*] k8s_example.py
```
