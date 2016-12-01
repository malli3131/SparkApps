import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import java.util.Date
import java.text.SimpleDateFormat
import java.text.DateFormat
import java.util.Locale

import scala.io.Source
import scala.collection.mutable.ListBuffer

object CustomSentiment {
	def main(args:Array[String]){
		val conf:SparkConf = new SparkConf().setAppName("Sentiment Analysis Application")
		val sc:SparkContext = new SparkContext(conf)
 		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._
	
		val inputArgs = new ListBuffer[String]()
		for(line <- Source.fromFile(args(0)).getLines())
		{
			inputArgs += line
		}
		val myArgs = inputArgs.toList
		val configArgs = sc.broadcast(myArgs)
		val tweetsdata = sc.textFile("/home/hadoop/srinu/tweets")
		val tweets = tweetsdata.map(record => getTimeBoundTweet(record.split("\t")(1), record.split("\t")(3), configArgs.value))
		val tweets_data = tweets.filter(record => record.length() > 3)
		val parsed_tweets = tweets_data.map(tweet => cleanTweet(tweet))
		val hashTags = configArgs.value(2)
		val hashTagTweets = parsed_tweets.filter(tweet => filterTweet(tweet, hashTags))
		val tweets_filtered = hashTagTweets.filter(line => line.length() > 5).toDF("tweet")
		val tweet_sentiment = tweets_filtered.select('tweet, sentiment('tweet).as('sentiment))
		val tweetSentiment = tweet_sentiment.select(sentimentClass('sentiment) as ('sentClass))
		val sentimentCounts = tweetSentiment.groupBy("sentClass").count()
		sentimentCounts.write.format("com.databricks.spark.csv").save("/home/hadoop/srinu/sentiment")
	}

	def getSentimentClass(sentimentIndex:Int) : String = {
                var sentimentClass = "Unknown"
                if(sentimentIndex < 0 && sentimentIndex > 4)
                sentimentClass = "Unknown"
                else if(sentimentIndex >= 0 && sentimentIndex <=1)
                sentimentClass = "Negitive"
                else if(sentimentIndex >1 && sentimentIndex <=2)
                sentimentClass = "Neutral"
                else if (sentimentIndex > 2 && sentimentIndex <= 4)
                sentimentClass = "Positive"
                return sentimentClass
        }
	
	def sentimentClass = udf { sentenceIndex: Int =>
		getSentimentClass(sentenceIndex)
        }

	def getStartTime(startTime:String) : Long = {
		val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.US);
                formatter.setLenient(false);
                val startDate = formatter.parse(startTime);
                val startMillis = startDate.getTime();
		startMillis
	}
	def getEndTime(endTime:String) : Long = {
		val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.US);
                formatter.setLenient(false);
                val endDate = formatter.parse(endTime);
                val endMillis = endDate.getTime();
		endMillis
	}
	def getTimeBoundTweet(tweet:String, tweet_time:String, myArgs:List[String]) : String = {
		val startTime = getStartTime(myArgs(0))
		val endTime = getEndTime(myArgs(1))
		val ttime = tweet_time.toLong
		if(ttime >= startTime && ttime <= endTime){
			return tweet
		}
		return "NA"
	}

	def filterTweet(tweet:String, hashtags:String) : Boolean = {
		val tags = hashtags.split(",")
		var flag = false
		for(tag <- tags){
			if(tweet.contains(tag)){
				flag = true
			}
		}
		return flag
	}

	def cleanTweet(tweet:String) : String = {
		var parsedTweet = tweet.replaceAll("""[\p{Punct}&&[^.#]]""", "")
		parsedTweet = parsedTweet.replaceAll("""[.]{1,}""", ".")
		return parsedTweet	
	}
}
