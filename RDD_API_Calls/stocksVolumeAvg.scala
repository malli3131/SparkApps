import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object stocksVolumeAvg{
    def main(args:Array[String]){
	val conf = new SparkConf().setAppName("Stocks Volume Avg").setMaster("local")
	val sc = new SparkContext(conf)
	val stocks = sc.textFile("./stocks")
	val projdata = stocks.map(value => (value.split("\t")(1).toLowerCase(), value.split("\t")(7).toInt))
	val combineData = projdata.combineByKey( value => (value, 1), (x:(Int,Int), value) => (x._1 + value, x._2 + 1), (x:(Int, Int),y:(Int, Int)) => (x._1 + y._1, x._2 + y._2))
	val avgRDD = combineData.map(stocks => (stocks._1, stocks._2._1, stocks._2._1/stocks._2._2))
	avgRDD.saveAsTextFile("./volavg")	
    }
}
