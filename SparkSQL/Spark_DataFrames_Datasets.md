Creating DataFrames:

	1. Using Sequences:

		val namesDF = Seq(("naga", 30), ("Ravi", 33), ("hari", 26)).toDF("name", "age")

	2. Using Existing RDDs:

		val namesRDD = sc.parallelize(Seq(("naga", 30), ("Ravi", 33), ("hari", 26)))
		val namesDF = namesRDD.toDF("name", "age")

		val namesRDD = sc.parallelize(Seq(Row("naga", 30), Row("Ravi", 33), Row("hari", 26)))
		val schema = StructType(List(StructField("name", StringType, true), StructField("age", IntegerType, true)))
		val namesDF = spark.createDataFrame(namesRDD, schema)

	3. Using Data sources:
			1. Hive Tables
			2. Json
			3. Parquet
			4. CSV
			5. Avro
			6. JBDC Databases
			7. Running SQL on files

		JDBC Databases:

		Note: Add the JDBC connector jar to SPARK_HOME/jars folder.

			val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/hadoop").option("dbtable", "hadoop.emp").option("user", "root").option("password", "root").load()
			jdbcDF.show()

			import java.util.Properties
			val conProps = new Properties()
			props.put("user", "root")
			props.put("password", "root")

			val jdbcDF = spark.read.jdbc("jdbc:mysql://localhost:3306/hadoop", "hadoop.emp", conProps)
			jdbcDF.show()


		Running SQL queries on files:

			Running SQL queries on files:

		val peopleDF = spark.sql("SELECT * FROM parquet.`hdfs://master:9000/sql/people.parquet`")
		peopleDF.show()


DataFrame Language:

DataFrame Methods and Attributes:

	1. show
		namesDF.show()

	2. filter
		namesDF.filter(namesDF("age") > 30).show()

	3. select

		namesDF.select("name").show()

	4. drop

		namesDF.drop("name").show()

	5. agg

		namesDF.agg(sum("age")).show()
		namesDF.agg(max("age")).show()
		namesDF.agg(min("age")).show()
		namesDF.agg(avg("age")).show()

	6. alias

		val names = namesDF.alias("names")

	7. sort

		namesDF.sort("age").show()

	8. describe

		namesDF.describe("age").show()
		namesDF.describe().show()

	9. printSchema

		namesDF.printSchema

	10. foreach

		namesDF.foreach(row => println(row.getString(0)))
		namesDF.foreach(row => println(row.getInt(1)))

	11. head

		namesDF.head

	12. take

		namesDF.take(1)

	13. limit

		namesDF.limit(2).show()

	14. write

		namesDF.write.csv("/home/naga/bigdata/jobs/names")

	15. distinct

		namesDF.distinct().show()
		namesDF.select("name").distinct().show()

	16. foreachPartition

		namesDF.foreachPartition(part => {
     		part.foreach(row => println(row.getString(0)))
     	})

    17. schema

    	namesDF.schema

    18. cache

    	namesDF.cache()

    19. groupBy

    	namesDF.groupBy("name").agg(sum("age")).show()
    	namesDF.groupBy("name").agg(count("age")).show()
		namesDF.groupBy("name").agg(max("age")).show()
		namesDF.groupBy("name").agg(min("age")).show()

	20. groupByKey

		namesDF.groupByKey(row => row.getString(0)).keys.show
		namesDF.groupByKey(row => row.getString(0)).count.show

	21. rdd

		val names = namesDF.rdd

	22. collect

		namesDF.collect

	23. hint

		namesDF.hint("names")

	24. map

		namesDF.map(row => row.getString(0)).collect

	25. stat

		namesDF.stat

	26. isLocal

		namesDF.isLocal

	27. isStreaming

		namesDF.isStreaming

	28. dtypes

		namesDF.dtypes

	29. inputFiles

		namesDF.inputFiles

	30	columns

		namesDF.columns

	31. storageLevel

		namesDF.storageLevel

	32. dropDuplicates

		namesDF.dropDuplicates.show()

	33. col

		namesDF.col("name")

	34. count

		namesDF.count

	35. where

		namesDF.where("age > 30").show()
		namesDF.where($"age" > 30).show()

	36. explain

		namesDF.explain - PP
		namesDF.explain(true) - AP

	37. selectExpr

		namesDF.selectExpr("name as pname", "age").show
		namesDF.selectExpr("name as pname", "age - 2").show

	38. sparkSession

		namesDF.sparkSession

	39. first

		namesDF.first

	40. sqlContext

		namesDF.sqlContext

	41 	orderBy

		namesDF.orderBy("age").show

	42. persist

		namesDF.persist()

	43.	withColumnRenamed

		namesDF.withColumnRenamed("name", "pname")

	44. withColumn

		namesDF.withColumn("upperName", addingColUDF(col("name"))(0)).show

		sample Spark SQL UDF:

			val addingColUDF = udf((s:String) => Array(s.toUpperCase(),s.toLowerCase()))

	45. createTempView

			namesDF.createTempView("people")
			spark.sql("select * from people").show

	46. createOrReplaceTempView

			namesDF.createOrReplaceTempView("people")
			spark.sql("select * from people").show

	47. createGlobalTempView

			namesDF.createGlobalTempView("info")
			spark.sql("select * from global_temp.info").show

	48. createOrReplaceGlobalTempView

			namesDF.createOrReplaceGlobalTempView("info")
			spark.sql("select * from global_temp.info").show

	49. crossJoin(namesDF)

			namesDF.crossJoin(namesDF).show

	50.	coalesce

			namesDF.coalesce(1)

	51. repartition

			namesDF.repartition(2)

	52. toDF

			namesDF.toDF().show

	53.	toJSON

			namesDF.toJSON.show

	54.	na

			namesDF.na
				drop   fill   replace -- for missing data

	55.	toString

			namesDF.toString

	56.	writeStream

			namesDF.writeStream
			writeStream can be called only on streaming Dataset/DataFrame
                                                           
    57. sample

    		namesDF.sample(true, 0.5).show   

    58. cube

    		 namesDF.cube("name", "age").count.show

    59.	toLocalIterator

   			namesDF.toLocalIterator.next

   	60. unpersist

   			namesDF.unpersist()

   	61	randomSplit

   			namesDF.randomSplit(Array(0.5, 0.5)).length

   	62.	randomSplitAsList

   			namesDF.randomSplitAsList(Array(0.5, 0.5), 1).size

   	63. rollup

   			namesDF.rollup("name", "age").count.show

   	64. except

   			namesDF.except(names1DF).show

   	65. queryExecution

   			namesDF.queryExecution

   	66.	as

   			val nDF = namesDF.as("names")

   	67.	checkpoint

   			sc.setCheckpointDir("/home/naga/bigdata/checkpoint")
   			namesDF.checkpoint

   	68.	apply

   			namesDF.apply("name")

   	69. transform

   			namesDF.transform(row => row.select("name")).show

   	70. intersect

   			namesDF.intersect(names1DF).show

   	71. join

   			namesDF.join(names1DF, "name").show
   			namesDF.join(names1DF, namesDF.col("name") === names1DF.col("name"), "left").show

   	72. joinWith

   			namesDF.joinWith(names1DF, namesDF.col("name") === names1DF.col("name")).show
	
	73.	union

			namesDF.union(names1DF).show


Saving DataFrames:

	Into Hive Tables:

		val volumeDF = spark.sql("select stock, sum(volume) as aggvolume from stocks group by stock")
		volumeDF.write.saveAsTable("india.volume")

	Into JDBC DataBases:

		MySQL:

			volumeDF.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/hadoop").option("dbtable", "hadoop.volume_one").option("user", "root").option("password", "root").save()

			import java.util.Properties
			val conProps = new Properties()
			props.put("user", "root")
			props.put("password", "root")

			volumeDF.write.jdbc("jdbc:mysql://localhost:3306/hadoop", "hadoop.volume", props)

	Into Files:

		Parquet:

			volumeDF.write.parquet("hdfs://master:9000/sql/volume.parquet")
			VvolumeDF.write.save("hdfs://master:9000/sql/volume.par") -- The default file type is parquet.
		CSV:
			
			volumeDF.write.csv("hdfs://master:9000/sql/volume.csv")

		JSON:

			volumeDF.write.json("hdfs://master:9000/sql/volume.json")
		
		ORC:

			volumeDF.write.orc("hdfs://master:9000/sql/volume.orc")



SparkSQL Dataset:

	Dataset is a strongly typed collection of domain-specific objects that can be transformed in parallel using functional or relational operations.
	Each Dataset also has an untyped view called a DataFrame, which is a Dataset of Row.

	Operations available on Datasets are divided into transformations and actions.
	Transformations are the ones that produce new Datasets, and actions are the ones that trigger computation and return results.

	Example transformations include map, filter, select, and aggregate (groupBy).
	Example actions count, show, or writing data out to file systems.

	Datasets are "lazy", i.e. computations are only triggered when an action is invoked.
	Internally, a Dataset represents a logical plan that describes the computation required to produce the data.
	When an action is invoked, Spark's query optimizer optimizes the logical plan and generates a physical plan for efficient execution in a parallel and distributed manner.
	To explore the logical plan as well as optimized physical plan, use the explain function.

	To efficiently support domain-specific objects, an Encoder is required.
	The encoder maps the domain specific type T to Spark's internal type system.

	For example, given a class Person with two fields, name (string) and age (int), an encoder is used to tell Spark to generate code at runtime to serialize the Person object into a binary structure.
	This binary structure often has much lower memory footprint as well as are optimized for efficiency in data processing (e.g. in a columnar format).
	To understand the internal binary representation for data, use the schema function.

	There are typically two ways to create a Dataset. The most common way is by pointing Spark to some files on storage systems, using the read function available on a SparkSession.

	Person class need to create.
   	val people = spark.read.parquet("path").as[Person]
   	Dataset<Person> people = spark.read().parquet("...").as(Encoders.bean(Person.class));
 
	Datasets can also be created through transformations available on existing Datasets.
	For example, the following creates a new Dataset by applying a filter on the existing one:

	val names = people.map(_.name)  // in Scala; names is a Dataset[String]
   	Dataset<String> names = people.map((Person p) -> p.name, Encoders.STRING));
 
	Dataset operations can also be untyped, through various domain-specific-language (DSL) functions defined in: Dataset (this class), Column, and functions. 
	These operations are very similar to the operations available in the DataFrame abstraction in R or Python.

	To select a column from the Dataset, use apply method in Scala and col in Java.	
   	val ageCol = people("age")  // in Scala
   	Column ageCol = people.col("age"); // in Java
 
	Note that the Column type can also be manipulated through its various functions.

   	// The following creates a new column that increases everybody's age by 10.
   	people("age") + 10  // in Scala
   	people.col("age").plus(10);  // in Java
 
	A more concrete example in Scala:
   		// To create Dataset[Row] using SparkSession
   		val people = spark.read.parquet("...")
   		val department = spark.read.parquet("...")

   		people.filter("age > 30")
     	.join(department, people("deptId") === department("id"))
     	.groupBy(department("name"), people("gender"))
     	.agg(avg(people("salary")), max(people("age")))
 
	and in Java:
   		// To create Dataset<Row> using SparkSession
   		Dataset<Row> people = spark.read().parquet("...");
   		Dataset<Row> department = spark.read().parquet("...");
		
		people.filter(people.col("age").gt(30))
     	.join(department, people.col("deptId").equalTo(department.col("id")))
     	.groupBy(department.col("name"), people.col("gender"))
     	.agg(avg(people.col("salary")), max(people.col("age")));

	Create datasets using Collections:

		case class Person(name:String, age:Int, place:String)
		val peopleDS = Seq(Person("naga", 30, "bangalore"), Person("hari", 24, "mysore")).toDS()
		peopleDS.show
		peopleDS.printSchema

		val namesDS = Seq("Naga", "Hari", "Siva").toDS()
		namesDS.show
		val namesDS = Seq("Naga", "Hari", "Siva").toDS().withColumnRenamed("value", "name")
		namesDS.show

		val persons = Seq("Naga", "Hari", "Siva")
		val personDS = spark.createDataset(persons)
		personDS.show

	Creating Datasets using RDDs

		val peopleRDD = sc.textFile("/home/naga/bigdata/jobs/spark/people")
		peopleRDD.first()
		val projRDD = peopleRDD.map(record =>{
			val cols = record.split(",")
			(cols(0), cols(1).toInt, cols(2))
		})
		projRDD.first
		val projDS = projRDD.toDS()
		projDS.show

		val projDS = projRDD.map(record => Person(record._1, record._2, record._3)).toDS()
		projDS.show

		import org.apache.spark.sql._
		val personDS = spark.createDataset
		
	Creating Datasets using Hive Tables:
	
		val stocksDF = spark.sql("select * from stocks")
		stockDF.first
		
		case class Stock(market:String, stock:String, sdate:String, open:Double, high:Double, low:Double, close:Double, volume:Long, adj_close:Double)
		val stockDS = stockDF.as[Stock]
		stockDS.show

	Creating Datasets using data sources like json, csv, parquet, orc, text, etc...
	
		Json:

			val jsonDF = spark.read.json("file:///home/naga/bigdata/jobs/people.json")
			jsonDF.show
			jsonDF.printSchema
		
			case class Emp(age:Long, name:String)
			val jsonDS = spark.read.json("file:///home/naga/bigdata/jobs/people.json").as[Emp]
			jsonDS.printSchema
			jsonDS.show
			jsonDS.write.csv("file:///home/naga/bigdata/jobs/person")

		CSV:

			case class Person(name:String, age:String, place:String)
			val peopleDF = spark.read.option("header", true).csv("hdfs://master:9000/sql/people.csv")
			val peopleDS = peopleDF.as[Person]
			peopleDS.show
			peopleDS.

	Running SQL queries on files:

		case class Person(name:String, age:String)
		val peopleDF = spark.sql("SELECT * FROM parquet.`hdfs://master:9000/sql/people.parquet`")
		val peopleDS = peopleDF.as[Person]
		peopleDS.show()
		peopleDS.printSchema
