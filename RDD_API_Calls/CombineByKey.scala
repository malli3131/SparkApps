import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf,SparkContext}

object CombineByKey{
    def main(args:Array[String]){
	val conf = new SparkConf().setAppName("RDD Aggregate").setMaster("local")
	val sc = new SparkContext(conf)
	val stocks = sc.textFile("./stocks")
	val projdata = stocks.map(line => (line.split("\t")(1), line.split("\t")(7).toInt))
	val aggRdd = projdata.combineByKey( value => (value, 1), (x:(Int,Int), value) => (x._1 + value, x._2 + 1), (x:(Int, Int),y:(Int, Int)) => (x._1 + y._1, x._2 + y._2))
	aggRdd.saveAsTextFile("./voulme")
    }
}
