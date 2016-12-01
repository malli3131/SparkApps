import re
import string
import sys

from pyspark import SparkContext

exclude = set(string.punctuation)

def get_hash_tag(word, rmPunc):
    pattern = re.compile("^#(.*)")
    m = pattern.match(word)
    tag = None
    if m:
        match = m.groups()
	for m_word in match:
	    tag = ''.join(letter for letter in m_word if letter not in rmPunc)
    if tag is not None:
	return tag

sc = SparkContext("local", "Finidng Hash Tags")
rmPunc = sc.broadcast(exclude)
mydata = sc.textFile("hdfs://ubuntu:9000/twitter/sample")
wordsRDD = mydata.flatMap( lambda line : line.split("\t")[1].split(" "))
tagsRDD = wordsRDD.map( lambda word : get_hash_tag(word, rmPunc.value)) 
hashtagsRDD = tagsRDD.filter( lambda word : word is not None) 
tagsCount = tagsRDD.map( lambda word : (word, 1)).reduceByKey( lambda a, b : a + b)
tagsCount.saveAsTextFile("hdfs://ubuntu:9000/twitter/hashtags")
