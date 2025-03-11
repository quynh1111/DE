from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Quynh_Local").setMaster("local[*]").set("spark.executor.memory", "1g")

sc= SparkContext(conf=conf)

# index là id của phân vùng
# func 1 logic lặp qua các phần tử trong index

numbersRdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10],2)
#(1,0),(2,0),........ (6,1),(7,1).......

# n,idx
"""
index : id of partition 
itertor: vòng lặp qua tất cả phần tử trong phân vùng

lambda:
idx: chỉ số của pratiton 
itr: vòng lặp qua tất cả phần tử trong phân vùng
n đối với mỗi phần tử trong partition thì nó tạo ra (n,idx) ghép nối lại với nhau
"""
result = (numbersRdd.mapPartitionsWithIndex(
    lambda idx, itr: [(n+1 if idx == 0 else n+2,idx )for n in itr] ))
print(result.collect())

