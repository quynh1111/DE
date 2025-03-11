
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Quynh_Local").setMaster("local[*]").set("spark.executor.memory", "1g")

sc= SparkContext(conf=conf)
"""
collect -  đưa dữ liệu vào list. [da ta]
- Không lấy ra được dữ liệu lớn

"""



"""
take - lấy ra từ đầu đến số đưa vào take

numbers = sc.parallelize([1,2,3,4,5,6,7,8,9,10],2)
# 1,2,3,4,5
# 6,7,8,9,10
print(numbers.take(6))
- [1,2,3,4,5,6,]

- đi từ phân vùng 1 lấy tất cả cá giá trị trong phân vùng 1
sang phân vùng 2 đến 6 thì in ra tất cả 
"""
"""
numbers = sc.parallelize([1,5,3,4,2,6,7,8,9,10],2)
#1,5,3,4,2
#6,7,8,9,10
print(numbers.take(6))
#[1, 5, 3, 4, 2, 6]
"""


"""
reduce - giảm 
- xử lý chặt chẻ.
- quy định kiểu dữ liệu cho đầu vào và đầu ra 
"""
"""
#xử lý thông thường
numbersRdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10],2)

def numberssum():
    sum= 0
    for i in numbersRdd.collect():
        sum += i
    return sum
print(numberssum())
"""
"""
#xử lý với reduce
# tính tổng từng phân vùng
# 1 2 3 4 5     3 3 4 5      6 4 5      10 5      15
# 6 7 8 9 10    13 8 9 10    21 9 10    30 10     40
# 15 40                                           50 

numbersRdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10],2)
def sum(v1:int,v2:int) -> int :
    print(f"v1:{v1}, v2:{v2} => ({v1+v2})")
    return v1+v2

print(numbersRdd.reduce(sum))
# kết quả 
# v1:1, v2:2 => (3)
# v1:3, v2:3 => (6)
# v1:6, v2:4 => (10)
# v1:10, v2:5 => (15)
# v1:6, v2:7 => (13)
# v1:13, v2:8 => (21)
# v1:21, v2:9 => (30)
# v1:30, v2:10 => (40)
# v1:15, v2:40 => (55)
# 55
"""
