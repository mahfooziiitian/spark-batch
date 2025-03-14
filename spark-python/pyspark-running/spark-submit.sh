#Using single script for launching it
./bin/spark-submit \
   --master yarn \
   --deploy-mode cluster \
   wordByExample.py

#Using multiple files
./bin/spark-submit \
   --master yarn \
   --deploy-mode cluster \
   --py-files file1.py,file2.py
   wordByExample.py

##Local files
spark-submit \
   --files file1,file2,file3


