from pyspark import SparkConf
from pyspark import SparkContext
conf = SparkConf().setMaster("local").setAppName("pyspark-wordcount")
sc = SparkContext(conf=conf)
fil = sc.textFile("f:/words.txt")
fi = fil.flatMap(lambda x: x.encode("ascii", "ignore").split(" ")).map(lambda x: (x, 1))\
    .reduceByKey(lambda x, y: (x+y))
sor = fi.sortBy(lambda x:  (x[1])
#fi.saveAsTextFile("f:/pysparkpro")
for x in sor.collect(): print x