from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Quynh_Local").setMaster("local[*]").set("spark.executor.memory", "1g")

sc= SparkContext(conf=conf)

# xóa hàm trùng lắp

valuaRdd= sc.parallelize(["one",1,"two",2,"three",3,"one","two",2,3])

print(valuaRdd.distinct().collect())
