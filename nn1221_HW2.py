from pyspark import SparkContext
import csv
import sys

def extractCuisine(partId, list_of_records):
    if partId==0:
        next(list_of_records)
    reader = csv.reader(list_of_records)
    for row in reader:
        (camis, cuisine) = (row[0], row[7])
        yield (camis, cuisine)

if __name__=='__main__':
    sc = SparkContext()
    filename = sys.argv[-2]
    rdd = sc.textFile(filename, use_unicode=False).cache()
    cuisine = rdd.mapPartitionsWithIndex(extractCuisine).distinct()
    counts = cuisine.map(lambda x: (x[1], 1)).reduceByKey(lambda x,y: x+y).top(25, key=lambda x: x[1])
    sc.parallelize(counts).saveAsTextFile(sys.argv[-1])
