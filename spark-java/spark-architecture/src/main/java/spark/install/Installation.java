/*

Step 1 : Ensure if Java is installed 

	java -version

Step 2 : Ensure if Scala is installed
	
	scala -version

Step 3 : If you don’t have Scala, then install it. Download Scala
	
Step 4 : Install Scala
	
	 tar xvf scala-2.11.6.tgz
	 
	 mv scala-2.11.6 /usr/local/scala
	 
	 exit
	 
	 
Step 5 : Set PATH for Scala using following command –
	
	export PATH = $PATH:/usr/local/scala/bin

Step 5 : Downloading Apache Spark
	
Step 6 : Installing Spark
	
	tar xvf spark-1.3.1-bin-hadoop2.6.tgz
	
	mv spark-1.3.1-bin-hadoop2.6 /usr/local/spark
	
	Configure the environment for Spark
	
	Add the following line to ~/.bashrc file which will add the location, where the spark software files are located to the PATH variable type.

		export PATH = $PATH:/usr/local/spark/bin

	Use the following below command for sourcing the ~/.bashrc file.

		source ~/.bashrc

Step 7 : Verify the Installation of Spark application on your system
	
	
	

 */
package spark.install;

public class Installation {

}
