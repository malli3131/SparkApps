import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf,SparkContext}

object CollectMap{
    def main(args:Array[String]){
	val conf = new SparkConf().setAppName("RDD Aggregate").setMaster("local")
	val sc = new SparkContext(conf)
	val citi = sc.textFile("./citi")
	val citiPairRDD = citi.map(row => (row.split("\t")(0), row.split("\t")(1).toInt)) 
	val data = citiPairRDD.collectAsMap()
	println(data)
    }
}
