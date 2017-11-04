package com.spark.mongo

import java.util.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bson.Document

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.config.WriteConfig

object MongoSparkUpdate extends App {
  val conf = new SparkConf().setAppName("Spark Mongo").setMaster("local[*]")
  val readOverrides = new HashMap[String, String]()
  readOverrides.put("spark.mongodb.input.uri", "mongodb://localhost:27017/info.people")
  val readConfig = ReadConfig.create(conf, readOverrides)
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().getOrCreate()

  val peopleRDD = MongoSpark.load(sc, readConfig)
  val updateRDD = peopleRDD.map { document => document.append("state", "karnataka") }
  val writeOverrides = new HashMap[String, String]()
  writeOverrides.put("spark.mongodb.output.uri", "mongodb://localhost:27017/info.people")
  writeOverrides.put("replaceDocument", "false")
  val writeConfig = WriteConfig.create(conf, writeOverrides)
  save(updateRDD, writeConfig)

  def save(rdd: RDD[Document], writeConfig: WriteConfig): Unit = {
    val mongoConnector = MongoConnector(writeConfig.asOptions)
    rdd.foreachPartition { partition =>
      {
        if (partition.nonEmpty) {
          mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
            {
              partition.foreach { document =>
                {
                  val searchDocument = new Document()
                  searchDocument.append("_id", document.get("_id").asInstanceOf[Double])
                  collection.replaceOne(searchDocument, document)
                }
              }
            }
          })
        }
      }
    }
  }
}
