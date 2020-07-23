import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
#spark = SparkSession.builder.getOrCreate()
spark = SparkSession.builder\
    .master("local") \
    .appName("My Spark Application")\
    .config("spark.submit.deployMode", "client")\
    .getOrCreate()

strings = spark.read.text("/opt/spark/README.md")
print(strings.show(10, truncate=False))

print(strings.count())

time.sleep( 60)
