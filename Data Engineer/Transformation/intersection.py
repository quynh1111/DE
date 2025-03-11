from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Quynh_Local").setMaster("local[*]").set("spark.executor.memory", "1g")

sc= SparkContext(conf=conf)

# tìm các phần tử giống nhau trong 2 rdd

rdd1= sc.parallelize([1,2,3,4,5])

rdd2= sc.parallelize([1,343,35,2,3,424])

rdd3= rdd1.intersection(rdd2)
print(rdd3.collect())