import org.apache.spark.sql.SparkSession

import com.mongodb.spark.MongoSpark
import org.elasticsearch.spark.rdd.EsSpark
import org.bson.Document

object SparkElastic extends App {
  case class EsDoc(stockId: Int, stock: String, date: String, open: Double, high: Double, low: Double, close: Double, volume: Int, adj_close: Double)
  val spark = SparkSession.builder().master("local").config("spark.mongodb.input.database", "nyse")
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/nyse.stocks").appName("Mongo Elastic")
    .config("es.nodes", "localhost:9200").getOrCreate();
  val sc = spark.sparkContext;
  val mongoRDD = MongoSpark.load(sc);
  val EsRDD = mongoRDD.map { doc => getEsDoc(doc) }
  EsSpark.saveToEs(EsRDD, "stocks/nyse");

  def getEsDoc(doc: Document): EsDoc = {
    val esDoc = EsDoc(doc.get("_id").asInstanceOf[Int], doc.get("stock").asInstanceOf[String], doc.get("date").asInstanceOf[String],
      doc.get("open").asInstanceOf[Double], doc.get("high").asInstanceOf[Double], doc.get("low").asInstanceOf[Double],
      doc.get("close").asInstanceOf[Double], doc.get("volume").asInstanceOf[Int], doc.get("adj_close").asInstanceOf[Double])
    (esDoc)
  }
}
