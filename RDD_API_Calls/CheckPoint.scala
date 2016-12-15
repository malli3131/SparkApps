import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf,SparkContext}

object Checkpoint{
    def main(args:Array[String]){
	val conf = new SparkConf().setAppName("RDD Aggregate").setMaster("local")
	val sc = new SparkContext(conf)
	sc.setCheckpointDir("./projdata")
	val stocks = sc.textFile("./stocks")
	val projdata = stocks.map(record => (record.split("\t")(1), record.split("\t")(7).toInt))
	projdata.checkpoint()
	println(projdata.count())
    }
}
