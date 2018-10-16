# from pyspark import SparkConf, SparkContext 
#conf = SparkConf().setAppName('hello').setMaster('spark://127.0.0.1:7077')
#conf = SparkConf().setAppName('hello').setMaster('spark://alex-HP-EliteBook-Folio-1040-G2:7077')
#sc = SparkContext(conf=conf)
from pyspark.sql import SparkSession
spark = SparkSession\
        .builder\
        .appName("TestSparkStandaloneCluster")\
        .getOrCreate()
x = ['spark', 'rdd', 'example', 'sample', 'example']
y = spark.sparkContext.parallelize(x)
#y = sc.parallelize(x)
print("-------------------------------------------\n")
print("The first element is %s \n" % y.first())
print("-------------------------------------------\n")


# the ui for the master
# http://127.0.0.2:8080/

# the environment variable
# export SPARK_HOME=/usr/local/spark

# start and stop all 
# $SPARK_HOME/sbin/start-all.sh
# $SPARK_HOME/sbin/stop-all.sh

# start the shell attached to the master
# $SPARK_HOME/bin/pyspark --master spark://alex-HP-EliteBook-Folio-1040-G2:7077

# start a standalone job
# $SPARK_HOME/bin/spark-submit --master spark://alex-HP-EliteBook-Folio-1040-G2:7077 /home/alex/python_programming/utt_formation/utt_formation_spark/spark_cluster.py 

# you want to do it yourself? 
# take a look at
# https://datawookie.netlify.com/blog/2017/07/installing-spark-on-ubuntu/
