import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf,SparkContext}

object Repartition{
    def main(args:Array[String]){
	val conf = new SparkConf().setAppName("RDD Aggregate").setMaster("local")
	val sc = new SparkContext(conf)
	val stocks = sc.textFile("./stocks", 4)
	println("stocks RDD partitions are: " + stocks.partitions.size)
	val stocksData = stocks.repartition(2)
	println("stocksdata RDD partitions are: " + stocksData.partitions.size)
    }
}
