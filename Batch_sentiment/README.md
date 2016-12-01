# SparkApps
Created a Spark application which analyses the tweets data.

The following steps are required to run this application:
	
	1. Get the tweets from Twitter using OAuth application and Flume
	2. Parse the tweets JSON to tab delimited file with specified columns/dimensions/attributes from every tweet - tweettime, tweet text, tweet language, tweet id, tweet hash tags, retweet text
		(id,text,lang,time,tags,retweet) --> Record Definition
	3. Compute the sentiment for these tweets using StandfordCoreNLP sentiment algorithm
		Usage: spark-submit --class CustomSentiment tweetsentiment.jar config_file 
