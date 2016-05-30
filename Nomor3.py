from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
import sys
import re
import time
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("Cluster")
sc = SparkContext(conf = conf)
jmlcluster = 20
jmliterasi =200
clustered = []
def parseLine(line):
    fields = re.split(r'\t+', line)
    idCitizen = float(fields[0])
    idunique = float(idCitizen/1000000000000000)
    jobType = fields[2].lower()
    basePay = float(fields[3])
    if basePay==0:
        basePay= 1
    overtimePay = float(fields[4])
    otherPay = float(fields[5])
    overtimePayNormal = float(overtimePay/basePay)
    if overtimePayNormal>5:
        overtimePayNormal = 5
    otherPayNormal = float(otherPay/basePay)/2
    if otherPayNormal>5:
        otherPayNormal = 5
    basepaynormalisasi = float(basePay/319275.01)
    return (idunique, basepaynormalisasi, overtimePayNormal, otherPayNormal, jobType)    

def multiple(data):
    idCitizen = float(data[3]*1000000000000000)
    idRow = int(idCitizen)
    return (idRow)
    
f = open('./result/data.txt','w+') 
f.truncate()   
lines = sc.textFile("file:///SparkCourse/salaries.txt")
filterdata = lines.filter(lambda x :"Not Provided" not in x)
parsedLines = filterdata.map(parseLine)
kerjaan = parsedLines.filter(lambda x: "police" in x[4])
paycluster = kerjaan.map(lambda x: (float(x[1]),float(x[2]),float(x[3]), float(x[0])))
#dataset = paycluster.collect()
#for data in dataset:
    #print data
#clustering starts
clusters = KMeans.train(paycluster, jmlcluster, maxIterations=200, runs=20, initializationMode="random")

#def error(point):
#    center = clusters.centers[clusters.predict(point)]
#   return sqrt(sum([x**2 for x in (point - center)]))
    
#print clusters.clusterCenters
datasets = paycluster.map(multiple)
dataset = datasets.collect()
i=0
arr = []
prediction = clusters.predict(paycluster)
predi = prediction.collect()
for pred in predi:
    arr.append(pred)
    #print pred
#time.sleep(5)
f.write("ID ROW\tNo.Cluster\n")
for data in dataset:
    data = str(data)
    data = data.translate(None, '() ')
    printed = data+"\t"+str(arr[i])+"\n"
    f.write(printed)
    i+=1
f.close()    
#WSSSE = paycluster.map(lambda x: (x, error(x)))
#print("Within Set Sum of Squared Error = " + str(WSSSE))
#datafinal = WSSSE.collect()
#for lala in datafinal:
    #print lala
#result = paybase.collect()
#maksbasepay = max(result)
#print maksbasepay