from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
import sys
import re
from pyspark import SparkConf, SparkContext

inputan = sys.argv[1]
inputint = int(inputan)
conf = SparkConf().setMaster("local").setAppName("GolonganJob")
sc = SparkContext(conf = conf)

def parseLine (line):
    fields = re.split(r'\t+', line)
    #fields = line.split("\t")
    jobType = fields[2]
    basePay = float(fields[3])
    return (jobType,basePay)
    
lines = sc.textFile("file:///SparkCourse/salaries.txt")
lines1 = lines.filter(lambda x : "Not Provided" not in x)
parsedLines = lines1.map(parseLine)
#filterdata = parsedLines.filter(lambda x : "Not Provided" not in x[1])
totalBasePay = parsedLines.mapValues(lambda x : (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averageBasePay = totalBasePay.mapValues(lambda x: x[0]/x[1]).map(lambda (x,y) : (y,x))
final= averageBasePay.map(lambda x: x[0])

payandjob = averageBasePay.collect()
results = final.collect()
maximum = max(results)
minimum = min(results)
sumrange = (maximum-minimum)/3

arrjob = []
for res in payandjob:
    if res[0] >= sumrange*2:
        arrjob.append((1,res))
    elif res[0]<sumrange:
        arrjob.append((3,res))
    else:
        arrjob.append((2,res))

for res in arrjob:
    if res[0]==inputint:
        print res
#for res in results:
#    print res
