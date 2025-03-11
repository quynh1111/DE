from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Quynh_Local").setMaster("local[*]").set("spark.executor.memory", "1g")

sc= SparkContext(conf=conf)

fileRdd = sc.textFile("../data/dataDE.txt")
print(fileRdd.collect())

print(f"number of data: {fileRdd.count()}")

print (f"first value data: {fileRdd.first()}")

