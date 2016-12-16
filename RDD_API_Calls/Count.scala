import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Count {
    def main(args:Array[String]){
	val conf:SparkConf = new SparkConf().setAppName("Count Example").setMaster("local")
	val sc:SparkContext = new SparkContext(conf)
	val data = sc.sequenceFile[String, Long]("/home/hadoop/bigdata/SparkApps/RDD_API_Calls/stocks.seq")
	val items = data.count()
	println("The number of records in the file: " + items)
    }
}
