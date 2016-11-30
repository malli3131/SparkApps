 #!/usr/bin/python
'''
required fileds
  
   1.lang
   2.id
   3.timestamp_ms
   4.text
   5.hashtags
   6.retweet_text
   
'''

from pyspark import SparkContext

import json
import re
import os
import sys
import unicodedata

def get_json_data(jsonString):
    jsondoc = json.loads(jsonString)
    record = []
    textrecord = None
    if jsondoc['lang'] == 'en':
        record.append(str(jsondoc['id']))
        tweet = unicodedata.normalize('NFKD', jsondoc['text']).encode('ascii', 'ignore')
        tweet = re.sub(' +', ' ', tweet)
        tweet = re.sub(r'(?:(?:\s|.|\[|)http|(?:\s|.|\[|)http).*(?:\s|\n|.|\r)', '', tweet)
        tweet = re.sub(' +', ' ', tweet)
        record.append(" ".join(tweet.split()))
        record.append(jsondoc['lang'])
        record.append(jsondoc['timestamp_ms'])
        if 'entities' in jsondoc.keys():
            tags = jsondoc['entities']['hashtags']
            for tag in tags:
                record.append(tag['text'])
        if 'retweeted_status' in jsondoc:
            retweets = unicodedata.normalize('NFKD', jsondoc['retweeted_status']['text']).encode('ascii', 'ignore')
            retweets = re.sub(' +', ' ', retweets)
            retweets = re.sub(r'(?:(?:\s|.|\[|)http|(?:\s|.|\[|)http).*(?:\s|\n|.|\r)', '', retweets)
            retweets = re.sub(' +', ' ', retweets)
            record.append(" ".join(retweets.split()))
        myrecord = "\t".join(record)
    	textrecord = unicodedata.normalize('NFKD', myrecord).encode('ascii', 'ignore')
    return textrecord


def getGoodRecord(inputdata):
    record = get_json_data(inputdata)
    if record != "":
	return record

sc = SparkContext("local", "Twitter data processing!")
mydata = sc.textFile("hdfs://<hostname:port>/<path>/FlumeData.1473867624870")
parsedata = mydata.map(getGoodRecord) 
result = parsedata.filter(lambda line: line is not None)
result.saveAsTextFile("hdfs://<hostname:port>/<path>/parsedata")
