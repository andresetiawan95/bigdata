from math import sqrt
from numpy import array
import sys
import re
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("LemburJob")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = re.split(r'\t+', line)
    jobType = fields[2]
    basePay = float(fields[3])
    if basePay==0:
        basePay=-1
    overtimePay = float(fields[4])
    payrate = float(overtimePay/basePay)
    return (jobType, payrate)

lines = sc.textFile("file:///SparkCourse/salaries.txt")
filterdata = lines.filter(lambda x :"Not Provided" not in x)
parsedLines = filterdata.map(parseLine)

averageOvertimePay = parsedLines.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
totalOvertimePay=averageOvertimePay.mapValues(lambda x: x[0] / x[1])

flippedresult = totalOvertimePay.map(lambda (x,y) : (y,x)).sortByKey()

results = flippedresult.collect()
maxlembur = max(results)[1]

print str(maxlembur)