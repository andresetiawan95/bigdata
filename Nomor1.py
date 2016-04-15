from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("GolonganJob")
sc = SparkContext(conf = conf)

def parseLine (line):
    fields = line.split(',')
    jobType = fields[2]
    basePay = fields[3]
    return (jobType, basePay)
    
lines = sc.textFile("file:///bigdata/salaries.csv")
parsedLines = lines.map(parseLine)
filterdata = parsedLines.filter('something yang ane masih belum paham')
averageOvertimePay = filterdata.