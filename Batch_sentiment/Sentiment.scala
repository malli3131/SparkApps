import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

object Sentiment {
	def main(args:Array[String]){
		val conf:SparkConf = new SparkConf().setAppName("Sentiment Analysis Application")
		val sc:SparkContext = new SparkContext(conf)
 		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._
		val tweetsdata = sc.textFile("/home/hadoop/srinu/tweets.csv")
		val tweets = tweetsdata.map(record => record.split(",")(1))
		val tweets_filtered = tweets.filter(line => line.length() > 5).toDF("tweet")
		val tweet_sentiment = tweets_filtered.select('tweet, sentiment('tweet).as('sentiment))
		val tweetSentiment = tweet_sentiment.select('tweet, sentimentClass('sentiment) as ('sentClass))
		tweetSentiment.write.format("com.databricks.spark.csv").save("/home/hadoop/srinu/sentiment")
	}

	def getSentimentClass(sentimentIndex:Int) : String = {
                var sentimentClass = "Unknown"
                if(sentimentIndex <= 0 && sentimentIndex > 4)
                sentimentClass = "Unknown"
                else if(sentimentIndex > 0 && sentimentIndex <=1)
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
}
