import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Histogram{
	def main(args:Array[String]){
		val conf:SparkConf = new SparkConf().setAppName("Histogram").setMaster("local")
		val sc:SparkContext = new SparkContext(conf)
		val dataset1:RDD[String] = sc.textFile("/home/hadoop/spark/scala/mllib/core/data1")
		val dataset2:RDD[String] = sc.textFile("/home/hadoop/spark/scala/mllib/core/data2");
		val subRDD:RDD[String] = dataset1.subtract(dataset2)
		val keyValueRDD:RDD[(String, String)] = subRDD.map(line => (line.split(",")(1), line.split(",")(0)))
		val hist = keyValueRDD.countByKey
		for((k,v) <- hist){
			println(k + "===>" + v)
		}
	}
}
