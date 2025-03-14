# Yarn cluster mode.
    
    spark-submit \
        --class my.main.Class \
        --master yarn \
        --deploy-mode cluster \
        --jars my-other-jar.jar,my-other-other-jar.jar \
        my-main-jar.jar \
        app_arg1 app_arg2


