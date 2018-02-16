## Spark MLLib Documentation

* Spark MLLib is library from Spark Project. It is an Ecosystem project under Spark.
* MLLib makes practical machine learning is easy at scale.
* MLLib is collection of Machine learning algorithms, feature engineering, tools and utilities.
* MLLib is avialable in two ways:
  * RDD based (Older One) - spark.mllib
  * DataFrame based (New one) - spark.ml
* MLLib offers the following things:
  * ML Algorithms: common learning algorithms such as classification, regression, clustering, and collaborative filtering
  * Featurization: feature extraction, transformation, dimensionality reduction, and selection
  * Pipelines: tools for constructing, evaluating, and tuning ML Pipelines
  * Persistence: saving and load algorithms, models, and Pipelines
  * Utilities: linear algebra, statistics, data handling, etc.
* The MLlib RDD-based API is now in maintenance mode and it is going to be removed once DataFrame based API gets feature parity.

### Basic Statistics:
* There are two basic statistics offered from MLLib
  * Correlation
  * Hypothesis testing

#### Correlation: 
* Correlation is a statistical technique that can show whether and how strongly pairs of variables are related.
  * ex:- weight vs height
* The main result of a correlation is called the correlation coefficient ("r"). It ranges from -1.0 to +1.0. The closer r is to +1 or -1, the more closely the two variables are related.
* If r is close to 0, it means there is no relationship between the variables. If r is positive, it means that as one variable gets larger the other gets larger. If r is negative it means that as one gets larger, the other gets smaller (often called an "inverse" correlation).

##### Examples:

```
package com.hub.bigdata.spark.mllib

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.stat.Correlation

object MyCorrelation extends App {

  val sparkSession = SparkSession.builder().appName("Correlation").master("local").getOrCreate()
  import sparkSession.implicits._

  val data = Seq(
    Vectors.sparse(4, Seq((0, 1.0), (1, 4.0), (2, 3.0),  (3, -2.0))),
    Vectors.dense(4.0, 5.0, 0.0, 3.0),
    Vectors.dense(6.0, 7.0, 0.0, 8.0),
    Vectors.sparse(4, Seq((0, 9.0), (3, 1.0))))

  val dataFrame = data.map(Tuple1.apply).toDF("features")
  val corrMatrix = Correlation.corr(dataFrame, "features")
  val scorrMatrix = Correlation.corr(dataFrame, "features", "spearman").head()

  println(scorrMatrix)
}
```
