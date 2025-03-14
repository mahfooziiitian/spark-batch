/*

We’ll show this by running a sample Scala application that comes with Spark.

	./bin/spark-submit \  --class org.apache.spark.examples.SparkPi \  --master local \  ./examples/jars/spark-examples_2.11-2.2.0.jar 10

We can also run a Python version of the application using the following command:
	./bin/spark-submit \  --master local \  ./examples/src/main/python/pi.py 10

By changing the master argument of spark-submit, we can also submit the same application to a cluster running Spark’s standalone cluster manager, Mesos or YARN.

./bin/spark-submit

--class <main-class>

--master <master-url>

--deploy-mode <deploy-mode>

--conf <key>=<value>

... # other options

<application-jar>

[application-arguments]


class: entry point for your application (e.g. org.apache.spark.examples.SparkPi)

master: master URL for the cluster (e.g. spark://23.195.26.187:7077)

deploy-mode: Whether to deploy your driver on the worker nodes (cluster) or locally as an external client (client) (default: client)

conf: Arbitrary Spark configuration property in key=value format.

application-jar: Path to a bundled jar with the application and dependencies.

application-arguments: Arguments passed to the main method of your main class, if any.


Deployment Modes
Choose which mode to run using the --deploy-mode flag

Client - Driver runs on a dedicated server (e.g.: Edge node) inside a dedicated process. Submitter starts the driver outside of the cluster.

Cluster - Driver runs on one of the cluster's Worker nodes. The Master selects the worker. The driver operates as a dedicated, standalone process inside the Worker.


	
 */
package spark.execution.submit;

public class SparkSubmit {

}
