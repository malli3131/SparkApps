import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf,SparkContext}

object Cogroup{
    def main(args:Array[String]){
	val conf = new SparkConf().setAppName("RDD Aggregate").setMaster("local")
	val sc = new SparkContext(conf)
	val citi = sc.textFile("./citi")
	val hdfc = sc.textFile("./hdfc")
	val sbi = sc.textFile("./sbi")
	val citiPairRDD = citi.map(row => (row.split("\t")(0), row.split("\t")(1).toInt)) 
	val hdfcPairRDD = hdfc.map(row => (row.split("\t")(0), row.split("\t")(1).toInt)) 
	val sbiPairRDD = sbi.map(row => (row.split("\t")(0), row.split("\t")(1).toInt)) 
	val groupRDD = citiPairRDD.cogroup(hdfcPairRDD, sbiPairRDD)
	groupRDD.collect.foreach{println}
    }
}
