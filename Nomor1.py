from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt

import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("GolonganJob")
sc = SparkContext(conf = conf)

def parseLine (line):
    fields = re.split(r'\t+', line)
    #fields = line.split("\t")
    jobType = fields[2]
    basePay = float(fields[3])
    return (jobType, basePay)
    
lines = sc.textFile("file:///SparkCourse/salaries.txt")
lines1 = lines.filter(lambda x : "Not Provided" not in x)
parsedLines = lines1.map(parseLine)
#filterdata = parsedLines.filter(lambda x : "Not Provided" not in x[1])
totalBasePay = parsedLines.mapValues(lambda x : (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averageBasePay = totalBasePay.mapValues(lambda x: x[0]/x[1])
results = averageBasePay.collect()
maximum = max(results)

for result in results:
   print result
   
print maximum