import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf,SparkContext}

object Coalesce{
    def main(args:Array[String]){
	val conf = new SparkConf().setAppName("RDD Aggregate").setMaster("local")
	val sc = new SparkContext(conf)
	val stocks = sc.textFile("./stocks", 4)
	println("stocks RDD partitions are: " + stocks.partitions.size)
	val stocksData = stocks.coalesce(2, false)
	println("stocksdata RDD partitions are: " + stocksData.partitions.size)
    }
}
