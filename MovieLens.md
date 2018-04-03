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


2 Top twenty rated movies (Condition: The movie should be rated/viewed by at least 40 users)

```
def getSumCount(rows:Iterable[Int]) : (Int, Int) = {
    var sum = 0
    var count = 0
    for(row <- rows){
        count = count + 1
        sum = sum + row
    }
    (sum , count)
}
val ratingsBaseRDD = sc.textFile("/home/naga/Downloads/ml-1m/ratings.dat")
val ratingsRDD = ratingsBaseRDD.map(record => (record.split("::")(1), record.split("::")(2).toInt))
val groupedRDD = ratingsRDD.groupByKey().map(record => (record._1, getSumCount(record._2))).filter(record => record._2._2 > 40)
val avgRatingsRDD = groupedRDD.map(record => (record._1, (record._2._1.toDouble/record._2._2.toDouble))).sortBy(record => record._2, false)
val top20 = avgRatingsRDD.take(20)
```
