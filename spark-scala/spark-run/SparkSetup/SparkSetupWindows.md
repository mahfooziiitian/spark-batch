1. **Install java 8**

        java -version

2. **Download Apache Spark** 

        Invoke-WebRequest -Uri `
            'https://archive.apache.org/dist/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz' `
            -OutFile 'C:\Users\Mohammed_Alam\softwares\ApacheSpark\spark-2.4.0-bin-hadoop2.7.tgz'

3. **Verify Spark Software File**

         certutil -hashfile C:\Users\Mohammed_Alam\softwares\ApacheSpark\spark-2.4.0-bin-hadoop2.7.tgz SHA512

4. **Install Apache Spark**

         Extract it.

5. **Add winutils.exe File**

         mkdir hadoop/bin

6. **Configure Environment Variables**
   
   HADOOP_HOME=
   SPARK_HOME=
