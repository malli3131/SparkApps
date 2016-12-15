import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf,SparkContext}

object Cartesian{
    def main(args:Array[String]){
	val conf = new SparkConf().setAppName("RDD Aggregate").setMaster("local")
	val sc = new SparkContext(conf)	
	val people = sc.textFile("./people")
	val country = sc.textFile("./country")
	val cartRDD = people.cartesian(country)
	cartRDD.collect().foreach{println}
    }
}
