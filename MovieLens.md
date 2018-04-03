1. Top ten most viewed movies with their movies Name (Ascending or Descending order)
Solution:
```
val ratingsRDD = sc.textFile("/home/naga/Downloads/ml-1m/ratings.dat")
val movieRDD = ratingsRDD.map(record => record.split("::")(1)).map(movie => (movie, 1))
val movieCountsRDD = movieRDD.reduceByKey((a, b) => a + b)
val movieNamesRDD = sc.textFile("/home/naga/Downloads/ml-1m/movies.dat").map(record => (record.split("::")(0), record.split("::")(1)))
val resultRDD = movieCountsRDD.join(movieNamesRDD).map(record => (record._1, record._2._1, record._2._2)).sortBy(record => record._2, false)
val topTenMovies = resultRDD.take(10)
```
