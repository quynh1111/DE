from pyspark import SparkContext, SparkConf
import time
from random import Random

conf = SparkConf().setAppName("Quynh_Local").setMaster("local[*]").set("spark.executor.memory", "1g")

sc= SparkContext(conf=conf)

data = ["quynh","dung", "tho", "sami"]

rdd= sc.parallelize(data)

def numsPartition(iterfor):
    # create 1 num for map partition data
    rand= Random(int(time.time()*1000)+ Random().randint(0,1000))
    return [f"{item}: {rand.randint(0,1000)}" for item in iterfor]
result = rdd.mapPartitions(numsPartition)
print(result.collect())