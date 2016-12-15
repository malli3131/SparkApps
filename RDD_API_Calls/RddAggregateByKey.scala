import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf,SparkContext}

object RddAggregateByKey{
    def main(args:Array[String]){
	val conf = new SparkConf().setAppName("RDD Aggregate").setMaster("local")
	val sc = new SparkContext(conf)
	val stocks = sc.textFile("./stocks")
	val projdata = stocks.map(line => (line.split("\t")(1), line.split("\t")(7).toInt))
	val volMax = projdata.aggregateByKey(0)(math.max(_,_), math.max(_,_))
	val volMin = projdata.aggregateByKey(100000000)(math.min(_,_), math.min(_,_))
	val aggRdd = volMax ++ volMin
	aggRdd.saveAsTextFile("./voulme")
    }
}
