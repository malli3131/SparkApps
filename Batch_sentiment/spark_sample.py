from pyspark import SparkContext

sc = SparkContext("local")
data = sc.textFile("/home/hadoop/Downloads/tweets.csv")
data.saveAsTextFile("/home/hadoop/Downloads/mytweets")
