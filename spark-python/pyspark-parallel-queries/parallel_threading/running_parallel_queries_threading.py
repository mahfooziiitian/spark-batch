import os
import sys
import threading
from pyspark import SparkContext, SparkConf


def task(sc, i):
    print(sc.parallelize(range(i * 10000)).count())


def run_multiple_jobs():
    conf = SparkConf().setMaster('local[*]').setAppName('appname')
    conf.set('spark.scheduler.mode', 'FAIR')
    sc = SparkContext(conf=conf)
    for i in range(4):
        t = threading.Thread(target=task, args=(sc, i))
        t.start()
        print('spark task', i, 'has started')


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    run_multiple_jobs()
