from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("LemburJob")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    jobType = fields[2]
    overtimePay = fields[4]
    return (jobType, overtimePay)

lines = sc.textFile("file:///bigdata/salaries.csv")
parsedLines = lines.map(parseLine)
filterdata = parsedLines.filter(lambda x : x[1] is not "Not Provided")
averageOvertimePay = filterdata.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(lambda x: x[0] / x[1])

flippedresult = averageOvertimePay.map(lambda (x,y) : (y,x))
averageOvertimePaySorted = flippedresult.sortByKey()

results = averageOvertimePaySorted.collect();

for result in results :
    print result