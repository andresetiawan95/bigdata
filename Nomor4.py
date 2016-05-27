from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
import sys
import re
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("Cluster")
sc = SparkContext(conf = conf)