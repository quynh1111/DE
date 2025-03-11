from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Quynh_Local").setMaster("local[*]").set("spark.executor.memory", "1g")

sc= SparkContext(conf=conf)

#union : hợp nhất 2 rdd lại với nhau
# đầu nối đít
rdd1= sc.parallelize([1,2,3,4,5])
rdd2= sc.parallelize([2,35,5322,423])

rdd3 = rdd1.union(rdd2)
print(rdd3.collect())
