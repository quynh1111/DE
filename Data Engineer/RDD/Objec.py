
#import library

from pyspark import SparkContext

# create SparkContext

sc = SparkContext( "local","Quynh_Local")

#creat object

data=[
    {"id:":1, "name": "Quynh"},
    {"id:":2, "name": "Quynh1"},
    {"id:":3, "name": "Quynh2"}
]

#create rdd from data

rdd= sc.parallelize(data)

print(rdd.collect()) # print list format

print(f"number of data: {rdd.count()}")

print (f"first value data: {rdd.first()}")
