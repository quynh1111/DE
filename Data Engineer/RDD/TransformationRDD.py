from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Quynh_Local").setMaster("local[*]").set("spark.executor.memory", "1g")

sc= SparkContext(conf=conf)

number=[1,2,3,4,5,6,7,8,9]

rdd =sc.parallelize(number,3)

#using transformation for create rdd
squaredRDD = rdd.map(lambda i: i*i)
#print(squaredRDD.collect())

filterRDD= rdd.filter(lambda i: i>4)
#print(filterRDD.collect())

flatmapRDD = rdd.flatMap(lambda i: [i,i*2])
print(flatmapRDD.collect())

