#

    spark-submit \
        --master <master-url> \
        --deploy-mode <deploy-mode> \
        --conf <key<=<value> \
        --driver-memory <value>g \
        --executor-memory <value>g \
        --executor-cores <number of cores>  \
        --jars  <comma separated dependencies>
        --class <main-class> \
        <application-jar> \
        [application-arguments]

