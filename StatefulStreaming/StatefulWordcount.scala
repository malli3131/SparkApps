package com.test.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils

object StatefulWordcount extends App {
  val conf = new SparkConf().setAppName("Stateful Wordcount").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(10))
  val kafkaParams = Map[String, String]("bootstrap.servers" -> "localhost:9092", "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer", "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer", "group.id" -> "mygroup", "auto.offset.reset" -> "earliest")
  val topics = Set("widas")
  val inputKafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
  val words = inputKafkaStream.transform { rdd =>
    rdd.flatMap(record => (record.value().toString.split(" ")))
  }
  val wordpairs = words.map(word => (word, 1))
  ssc.checkpoint("/Users/nagainelu/bigdata/jobs/WordCount_checkpoint")
  val updateFunc = (values: Seq[Int], state: Option[Int]) => {
    val currentCount = values.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)
  }
  val wordCounts = wordpairs.reduceByKey(_ + _).updateStateByKey(updateFunc)
  wordCounts.print()
  ssc.start()
  ssc.awaitTermination()
}
