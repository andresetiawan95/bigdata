from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
import sys
import re
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("Cluster")
sc = SparkContext(conf = conf)
jmlcluster = 10
jmliterasi =200
clustered = []
def parseLine(line):
    fields = re.split(r'\t+', line)
    idCitizen = float(fields[0])
    idunique = float(idCitizen/1000000000)
    citizen = fields[1]
    jobType = fields[2].lower()
    basePay = float(fields[3])
    if basePay==0:
        basePay= 1
    overtimePay = float(fields[4])
    otherPay = float(fields[5])
    totalpaynormalisasi = float((overtimePay + otherPay)/basePay)
    basepaynormalisasi = float(basePay/319275.01)
    return (idunique, citizen, jobType, basePay, overtimePay, otherPay, totalpaynormalisasi, basepaynormalisasi)
    
lines = sc.textFile("file:///SparkCourse/salaries.txt")
filterdata = lines.filter(lambda x :"Not Provided" not in x)
parsedLines = filterdata.map(parseLine)
kerjaan = parsedLines.filter(lambda x: "police" in x[2])
paycluster = kerjaan.map(lambda x: (float(x[7]),float(x[6]),float(x[0])))
#dataset = paycluster.collect()

#clustering starts
clusters = KMeans.train(paycluster, jmlcluster, maxIterations=200, runs=20, initializationMode="random")

def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))
    
print clusters.clusterCenters
WSSSE = paycluster.map(lambda x: (x, error(x)))
#print("Within Set Sum of Squared Error = " + str(WSSSE))
datafinal = WSSSE.collect()
for lala in datafinal:
    print lala
#result = paybase.collect()
#maksbasepay = max(result)
#print maksbasepay