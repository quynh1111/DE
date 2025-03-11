from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Quynh_Local").setMaster("local[*]").set("spark.executor.memory", "1g")

sc= SparkContext(conf=conf)

text= sc.parallelize(["Dit Me May Lo ma hoc di"]) \
    .flatMap(lambda t : t.split(" ")) \
    .map(lambda l : l.lower())

#print(text.collect())

removeText = sc.parallelize(["dit me "]) \
    .flatMap(lambda x : x.split(" "))

niceText = text.subtract(removeText)
print(niceText.collect())
