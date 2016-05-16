from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
import sys

#boilerplate
inputan = sys.argv[1]
conf = SparkConf().setMaster("local").setAppName("SparkTFIDF")
sc = SparkContext(conf = conf)

#cobaInputan = cobainput.inputan()

#load documents (one per line)
hashing = HashingTF(100000)
fields = sc.textFile("berita.txt").map(lambda x: x.split("\t"))
documents = fields.map(lambda x: x[2].lower().split(" "))
namaDocument = fields.map(lambda x: x[0])


tf = hashing.transform(documents).cache();
idf = IDF(minDocFreq=2).fit(tf);
tfidf = idf.transform(tf)

keywordTF = hashing.transform([inputan.lower()])
keywordHashValue = int(keywordTF.indices[0])
keywordRelevance = tfidf.map(lambda x: x[keywordHashValue])
zipped = keywordRelevance.zip(namaDocument)

print "Dokumen yang cukup relevan untuk keyword tersebut adalah "
print zipped.max()[1]