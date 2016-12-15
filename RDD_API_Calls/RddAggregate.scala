import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf,SparkContext}

object RddAggregate{
    def main(args:Array[String]){
	val conf = new SparkConf().setAppName("RDD Aggregate").setMaster("local")
	val sc = new SparkContext(conf)
	val stocks = sc.textFile("./stocks")
	val projdata = stocks.map(line => line.split("\t")(7).toInt)
	val volMax = projdata.aggregate(0)(math.max(_,_), math.max(_,_))
	val volMin = projdata.aggregate(0)(math.min(_,_), math.min(_,_))
	println("The maximum stock value is: " + volMax + " Mininum Volume is: " + volMin)
    }
}
