from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Quynh_Local").setMaster("local[*]").set("spark.executor.memory", "1g")

sc= SparkContext(conf=conf)

fileRdd = sc.textFile("../data/dataDE.txt")

flatmapRdd= fileRdd.flatMap(lambda line: line.split(" "))

for line in flatmapRdd.collect():
    print(flatmapRdd.collect())

