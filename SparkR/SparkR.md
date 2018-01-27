SparkR

	Overview:

		1.	SparkR is an R package that provides a light-weight frontend to use Apache Spark from R.
		2.	In Spark 2.2.1, SparkR provides a distributed data frame implementation that supports operations like selection, filtering, aggregation etc. (similar to R data frames) but on large datasets.
		3.	SparkR also supports distributed machine learning using Spark MLlib.
		4. SparkR is an abstraction on top of Spark Core.

	SparkDataFrame
		
		SparkDataFrame is a distributed collection of data organized into named columns.
		It is conceptually equivalent to a table in a relational database or a data frame in R, but with richer optimizations under the hood.
		SparkDataFrames can be constructed from a wide array of sources such as: structured data files like json, orc, csv, parquet etc..., Hive tables, external databases, or existing local R data frames.

	SparkSession
		
		The entry point into SparkR is the SparkSession which connects your R program to a Spark cluster.
		We can create a SparkSession using sparkR.session and pass in options such as the application name, any spark packages depended on, etc.
		We can also work with SparkDataFrames via SparkSession.
		For sparkR shell, the SparkSession should already be created for us, and we would not need to call sparkR.session.

			sparkR.session()
			sparkR.session(appName = "R Spark SQL basic example", sparkConfig = list(spark.some.config.option = "some-value"))

			creating SparkSession in R standalone applications like Rscripts or RStudio

				if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  					Sys.setenv(SPARK_HOME = "/home/spark")
				}
				
				library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
				sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))



SparkR Hands On...

	Run shell --> sparkR --master <masterURL>
	sparksession is available as "spark"


	Creating DataFrames:

	Creating dataframes using local R dataframes:

		df <- as.DataFrame(faithful)
		head(df)

		csvdata <- read.csv("file:///home/naga/bigdata/jobs/people.csv")
		dataFrame = as.DataFrame(csvdata)
		head(dataFrame)
		head(select(dataFrame, "name"))
		head(select(dataFrame, dataFrame$name))

	Hive Tables:

		dataFrame <- sql("select * from stocks")
		head(dataFrame)
		printSchema(df)
		head(select(df, "stock"))

	Using Data sources:

		JSON:

			peopleDF <- read.df("file:///home/naga/bigdata/jobs/people.json", "json")
			head(peopleDF)
			printSchema(peopleDF)

			peopleDF <- read.json("file:///home/naga/bigdata/jobs/people.json")
			head(peopleDF)
			printSchema(peopleDF)

		Parquet:

			peopleDF <- read.df("hdfs://master:9000/sql/people.parquet", "parquet")
			peopleDF = read.df("hdfs://master:9000/sql/people.parquet", "parquet")
			read.df("hdfs://master:9000/sql/people.parquet", "parquet") -> peopleDF
			head(peopleDF)

		CSV:

			peopleDF <- read.df("hdfs://master:9000/sql/people.csv", "csv", header = "true", inferSchema = "true")
			head(peopleDF)

	Running SQL queries on files:

		peopleDF <- sql("SELECT * FROM parquet.`hdfs://master:9000/sql/people.parquet`")
		head(peopleDF)

	JDBC Databases:

		MySQL:
		
			empDF <- read.jdbc("jdbc:mysql://localhost:3306/hadoop", "hadoop.emp", user="root", password="root")

	Saving DataFrames:

		Into Hive Tables:

			saveAsTable(peopleDF, "person")

		Into files:

			JSON:

				write.json(peopleDF, "hdfs://master:9000/sql/Rpeople.json")

			Parquet:

				write.parquet(peopleDF, "hdfs://master:9000/sql/Rpeople.parquet")
				write.df(peopleDF, "hdfs://master:9000/sql/Rpeople") -- The default file type is parquet

			CSV:

				write.df(peopleDF, "hdfs://master:9000/sql/Rpeople.csv", "csv")

		Into JDBC Databases:

			MySQL:

				write.jdbc(projDF, "jdbc:mysql://localhost:3306/hadoop", "hadoop.employee", user="root", password="root")


	DataFrame Operations:

		1. head:

			head(empDF)

		2. select:

			head(select(empDF, "ename"))
			head(select(empDF, empDF$ename))

		3. filter:

			head(filter(empDF, empDF$sal > 2000))
			head(select(filter(empDF, empDF$sal > 2000), empDF$ename))

		4. groupBy and summarize

			head(summarize(groupBy(empDF, empDF$job), count=n(empDF$job)))

		5. arrange:

			head(arrange(job_counts, desc(job_counts$count)))
			head(arrange(job_counts, asc(job_counts$count)))

		6. column Ops:

			empDF$sal <- empDF$sal + 10000
			head(empDF)

		etc...

		Running SQL Queries in SparkR

		createOrReplaceTempView(empDF, "emp")
		job_counts <- sql(select job, count(*) from emp group by job")
		head(job_counts)
