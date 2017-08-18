package com.data.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Duration
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import org.apache.spark.streaming.twitter.TwitterUtils

object TwitterHashTags extends App {

  val conKey = "dhpRJpKOSJZyKRrm8zOlMXE6V"
  val conSrt = "AeM9uYgqW8Ij3XJQwGPcz8Co4AIT9ceBMSADeU6clDDrOGgxcQ"
  val at = "78572973-7VuCP3oITUO5W5p0zw19dSjBKpe3KFI0TtERlD5WW"
  val atc = "WrFfwjGBPARhaIzPHSD2MfP67t689wuB8WPK1iknhjYxS"

  val cb = new ConfigurationBuilder()
  cb.setDebugEnabled(true).setOAuthConsumerKey(conKey).setOAuthConsumerSecret(conSrt).setOAuthAccessToken(at).setOAuthAccessTokenSecret(atc)

  val auth = new OAuthAuthorization(cb.build())

  val conf = new SparkConf().setAppName("Twitter Sentiment").setMaster("local[*]")
  val ssc = new StreamingContext(conf, new Duration(60000))
  ssc.sparkContext.setLogLevel("ERROR")

  val tweets = TwitterUtils.createStream(ssc, Some(auth))
  val en_tweets = tweets.filter { tweet => tweet.getLang.equals("en") }
  val tweet_htags = en_tweets.map(_.getText).flatMap(text => text.split(" ")).filter(tags => tags.startsWith("#"))
  val trends = tweet_htags.map(tag => (tag, 1)).reduceByKeyAndWindow((a: Int, b: Int) => (a + b), new Duration(600000), new Duration(60000))
  trends.print()
  ssc.start()
  ssc.awaitTermination()
}
