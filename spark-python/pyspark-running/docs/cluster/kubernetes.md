# Kubernetes

Kubernetes (K8s) is the modern, cloud-native way to run Spark. The native K8s
scheduler has been included since Spark 2.3 — no Hadoop required.

## Architecture

```
┌──────────────────────────────────────────────────┐
│  Kubernetes Cluster                              │
│                                                  │
│  ┌────────────────┐  spawns  ┌────────────────┐  │
│  │  Driver Pod    │ ───────► │ Executor Pod 1 │  │
│  │                │          ├────────────────┤  │
│  │ (spark-submit  │          │ Executor Pod 2 │  │
│  │  --deploy      │          ├────────────────┤  │
│  │   cluster)     │          │ Executor Pod N │  │
│  └────────────────┘          └────────────────┘  │
└──────────────────────────────────────────────────┘
```

## Prerequisites

1. A running Kubernetes cluster (minikube, EKS, GKE, AKS, …)
2. `kubectl` configured — `~/.kube/config` present
3. A Docker image containing your PySpark code

## Build the Docker image

```dockerfile title="cluster/kubernetes/Dockerfile"
--8<-- "cluster/kubernetes/Dockerfile"
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

=== "Cluster mode"
    ```bash
    spark-submit \
      --master k8s://https://$(kubectl config view --minify \
                  -o jsonpath='{.clusters[0].cluster.server}') \
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

    !!! note "`local://` path"
        The `local://` prefix refers to a path **inside the container image**,
        not your local filesystem.

=== "Minikube quick-start"
    ```bash
    minikube start --cpus 4 --memory 8192
    eval $(minikube docker-env)           # use minikube's Docker daemon

    docker build -t pyspark-job:latest .

    spark-submit \
      --master k8s://https://$(minikube ip):8443 \
      --deploy-mode cluster \
      --conf spark.kubernetes.container.image=pyspark-job:latest \
      --conf spark.kubernetes.container.image.pullPolicy=Never \
      --conf spark.executor.instances=2 \
      local:///opt/spark/work-dir/k8s_example.py
    ```

=== "Local test (no cluster)"
    ```bash
    spark-submit --master local[*] cluster/kubernetes/k8s_example.py
    ```

## Monitor pods

```bash
kubectl get pods                             # driver + executor pods
kubectl logs <driver-pod-name>              # stream driver logs
kubectl logs <driver-pod-name> -f           # follow logs live
kubectl describe pod <driver-pod-name>      # debug scheduling issues
kubectl delete pod <driver-pod-name>        # force-terminate
```

## Common Kubernetes configuration

| Config key | Description |
|------------|-------------|
| `spark.kubernetes.container.image` | Docker image for driver and executors |
| `spark.kubernetes.namespace` | K8s namespace to run pods in |
| `spark.kubernetes.executor.request.cores` | CPU request per executor pod |
| `spark.kubernetes.executor.limit.cores` | CPU hard limit per executor pod |
| `spark.kubernetes.node.selector.<label>` | Pin pods to specific node pools |
| `spark.kubernetes.executor.volumes.*` | Mount PVCs or ConfigMaps |
| `spark.kubernetes.driver.podTemplateFile` | Custom driver pod spec |
| `spark.kubernetes.executor.podTemplateFile` | Custom executor pod spec |

## Example — `k8s_example.py`

```python title="cluster/kubernetes/k8s_example.py"
--8<-- "cluster/kubernetes/k8s_example.py"
```
