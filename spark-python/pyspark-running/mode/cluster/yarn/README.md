# Spark submit yarn

```bash
./bin/spark-submit \
   --master yarn \
   --deploy-mode cluster \
   wordByExample.py
```

## Using multiple files

```bash
./bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --py-files file1.py,file2.py \
    wordByExample.py
```

## Local files

```bash
spark-submit \
    --files file1,file2,file3
```
