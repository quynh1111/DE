from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Quynh_Local").setMaster("local[*]").set("spark.executor.memory", "1g")

sc= SparkContext(conf=conf)

fileRdd = sc.textFile("../data/dataDE.txt")

#usinh transformation

allcapsRdd= fileRdd.map(lambda line: line.upper())

for line in allcapsRdd.collect():
    print(line)