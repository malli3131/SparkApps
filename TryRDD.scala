package hub.naga.spark

import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TryRDD extends App {
  val conf = new SparkConf().setAppName("Try RDD Example").setMaster("local");
  val sc = new SparkContext(conf)

  val mydata = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 0))
  val mulRDD = mydata.map { x => x * 100 }
  val divRDD = mydata.map { x => Try(100 / x) }.map(x => x.flatMap { y => Try(y * 100) })
  divRDD.foreach(println)
}
